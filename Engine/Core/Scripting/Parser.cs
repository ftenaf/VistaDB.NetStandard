using System;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class Parser : IDisposable
  {
    private static char[] endLine = new char[1]{ '\n' };
    private SignatureList signatures;
    private DirectConnection connection;
    private EvalStack evalStack;
    private bool exactEqual;
    private int thumbOffset;
    private DataStorage activeStorage;
    private bool isDisposed;

    internal Parser(DirectConnection connection)
    {
      this.connection = connection;
      signatures = DoCreateSignatures();
    }

    private void RaiseError(int error)
    {
      RaiseError((Exception) null, error, (string) null);
    }

    private void RaiseError(Exception ex, int error, string message)
    {
      try
      {
        throw evalStack == null ? new VistaDBException(ex, error, message) : new VistaDBException(ex, error, new string(evalStack.Expression).Substring(thumbOffset));
      }
      finally
      {
        if (evalStack != null)
          evalStack = (EvalStack) null;
      }
    }

    private bool ParseSubset(int from, int to)
    {
      for (int index = from; index < to; ++index)
      {
        Signature signature = signatures[index];
        int num = signature.IncludedName(evalStack.Expression, thumbOffset, activeStorage.Culture);
        if (num > 0)
        {
          evalStack.PushCollector(signature);
          thumbOffset += num;
          return true;
        }
      }
      return false;
    }

    private char Thumb(int offset)
    {
      return evalStack.Expression[thumbOffset + offset];
    }

    private int ExtractTo(char finalChar)
    {
      int thumbOffset = this.thumbOffset;
      int index = thumbOffset;
      for (int length = evalStack.Expression.Length; index < length; ++index)
      {
        if (finalChar.Equals(evalStack.Expression[index]))
          return index - thumbOffset + 1;
      }
      return 0;
    }

    private void BypassSpaces(char spaceSymbol)
    {
      int length = evalStack.Expression.Length;
      while (thumbOffset < length && spaceSymbol.CompareTo(Thumb(0)) >= 0)
        ++thumbOffset;
    }

    private bool EqualPattern(char[] source, int sourceOffset, char[] destination, int destinationOffset, int len)
    {
      while (sourceOffset < source.Length && destinationOffset < destination.Length && source[sourceOffset++].Equals(destination[destinationOffset++]))
        --len;
      return len == 0;
    }

    private void BypassComments(char[] endGroup)
    {
      int length = evalStack.Expression.Length;
      while (thumbOffset < length && !EqualPattern(evalStack.Expression, thumbOffset, endGroup, 0, endGroup.Length))
        ++thumbOffset;
    }

    private bool TestSquareBrackets(ref int expectedNameLength, ref int extraLen)
    {
      if (Thumb(0) != '[')
        return false;
      extraLen = 2;
      expectedNameLength = ExtractTo(']') - extraLen;
      if (expectedNameLength <= 0)
        RaiseError(286);
      return true;
    }

    private bool ParseCommentsAndColumns(char[] bgnOfGroup, char spaceSymbol, DataStorage activeStorage)
    {
      BypassSpaces(spaceSymbol);
      int length = evalStack.Expression.Length;
      int num = length - thumbOffset;
      if (bgnOfGroup != null)
      {
        if (num <= 0 || !EqualPattern(bgnOfGroup, 0, evalStack.Expression, thumbOffset, bgnOfGroup.Length))
          return false;
        thumbOffset += bgnOfGroup.Length;
        BypassSpaces(spaceSymbol);
        num = length - thumbOffset;
      }
      if (num == 0)
        return false;
      if (signatures[signatures.COMMENTS_INLINE].IncludedName(evalStack.Expression, thumbOffset, this.activeStorage.Culture) > 0)
      {
        BypassComments(endLine);
        num = length - thumbOffset;
      }
      if (num == 0)
        return false;
      Signature signature = signatures[signatures.COMMENTS];
      if (signature.IncludedName(evalStack.Expression, thumbOffset, this.activeStorage.Culture) > 0)
      {
        BypassComments(signature.EndOfGroup);
        num = length - thumbOffset;
      }
      if (num == 0 || this.activeStorage == null)
        return false;
      int expectedNameLength = 0;
      int extraLen = 0;
      Row.Column sourceColumn;
      if (TestSquareBrackets(ref expectedNameLength, ref extraLen))
      {
        sourceColumn = this.activeStorage.LookForColumn(new string(evalStack.Expression, thumbOffset + 1, expectedNameLength));
        if (sourceColumn == (Row.Column) null)
          return false;
      }
      else
      {
        sourceColumn = this.activeStorage.LookForColumn(evalStack.Expression, thumbOffset, false);
        if (sourceColumn == (Row.Column) null)
          return false;
        expectedNameLength = sourceColumn.Name.Length;
      }
      evalStack.PushColumn(activeStorage, sourceColumn);
      thumbOffset += expectedNameLength + extraLen;
      return true;
    }

    private bool ParsePatterns()
    {
      if (thumbOffset < evalStack.Expression.Length)
        return OnParsePatterns();
      return false;
    }

    private bool ParseOperands()
    {
      if (thumbOffset < evalStack.Expression.Length)
        return OnParseOperands();
      return false;
    }

    private bool ParseTableNames(DataStorage activeStorage, char spaceChar)
    {
      int length = evalStack.Expression.Length;
      if (length == thumbOffset)
        return false;
      BypassSpaces(spaceChar);
      if (length == thumbOffset)
        return false;
      int expectedNameLength = 0;
      int extraLen = 0;
      if (TestSquareBrackets(ref expectedNameLength, ref extraLen))
      {
        if (connection.LookForTable(new string(evalStack.Expression, thumbOffset + 1, expectedNameLength)) == null)
          return false;
      }
      else
      {
        DataStorage dataStorage = connection.LookForTable(evalStack.Expression, thumbOffset, false);
        if (dataStorage == null)
          return false;
        expectedNameLength = dataStorage.Name.Length;
      }
      thumbOffset += expectedNameLength + extraLen;
      return true;
    }

    private bool CheckIFClause(Signature signature, ref bool ifGroup, ref bool elseExpected)
    {
      if (signature.Group == signatures.IF)
        evalStack.InsertEndOfGroup(signatures.PARENTHESIS + 1);
      if (ifGroup)
      {
        if (signature.Group != signatures.THEN)
        {
          thumbOffset -= signature.Name.Length;
          RaiseError(293);
        }
        ifGroup = false;
        elseExpected = true;
        return false;
      }
      if (signature.Group == signatures.THEN)
      {
        thumbOffset -= signature.Name.Length;
        RaiseError(294);
      }
      if (signature.Group == signatures.ELSE)
      {
        if (!elseExpected)
        {
          thumbOffset -= signature.Name.Length;
          RaiseError(295);
        }
        elseExpected = false;
        return true;
      }
      if (elseExpected)
      {
        evalStack.InsertElseGroup();
        elseExpected = false;
      }
      return false;
    }

    private bool ParseExpression(ref int delimitersCount, DataStorage activeStorage, char[] bgnOfGroup, char spaceChar, char[] delimiter)
    {
      evalStack.PushBreak();
      bool flag1 = true;
      bool ifGroup = false;
      bool elseExpected = false;
      while (ParseCommentsAndColumns(bgnOfGroup, spaceChar, activeStorage) || ParsePatterns() || (ParseOperands() || ParseTableNames(activeStorage, spaceChar)))
      {
        bgnOfGroup = (char[]) null;
        PCodeUnit unit = evalStack.PopCollector();
        Signature signature1 = unit.Signature;
        switch (signature1.Operation)
        {
          case Signature.Operations.BgnGroup:
            bool flag2 = CheckIFClause(signature1, ref ifGroup, ref elseExpected);
            evalStack.InsertEndOfGroup(signature1.EndOfGroupEntry);
            int delimitersCount1 = 0;
            unit.ContentBgn = thumbOffset;
            if (!ParseExpression(ref delimitersCount1, unit.ActiveStorage == null ? activeStorage : unit.ActiveStorage, signature1.BgnOfGroup, signature1.SpaceChar, signature1.Delimiter))
              return false;
            unit.ContentEnd = thumbOffset;
            if (evalStack.PopCollector().Signature != signatures[signature1.EndOfGroupEntry])
              RaiseError(286);
            ifGroup = signature1.Group == signatures.IF && delimitersCount1 == 0;
            unit.DelimitersCount = delimitersCount1;
            evalStack.PushPCode(unit);
            if (flag2)
              evalStack.InsertIfGroupFinalization();
            flag1 = false;
            continue;
          case Signature.Operations.EndGroup:
            evalStack.ReleaseCollector();
            evalStack.PopCollector();
            evalStack.PushCollector(unit);
            return true;
          case Signature.Operations.Delimiter:
            if (!signature1.IsSameName(delimiter, 0, this.activeStorage.Culture))
              RaiseError((Exception) null, 287, new string(delimiter));
            ++delimitersCount;
            evalStack.ReleaseCollector();
            continue;
          default:
            Signature signature2 = evalStack.PeekCollector().Signature;
            if (flag1 && signature1.UnaryOverloading && signature2.AllowUnaryToFollow)
            {
              signature1 = signatures[signature1.UnaryEntry];
              unit.Signature = signature1;
            }
            for (; signature2.Priority >= signature1.Priority && signature2.Group != signatures.BREAK; signature2 = evalStack.PeekCollector().Signature)
              evalStack.PushPCode(evalStack.PopCollector());
            evalStack.PushCollector(unit);
            continue;
        }
      }
      if (evalStack == null)
        return false;
      if (ifGroup)
        RaiseError(293);
      if (elseExpected)
        evalStack.InsertElseGroup();
      evalStack.ReleaseCollector();
      evalStack.PopCollector();
      return true;
    }

    private int SignaturesValidation(bool checkBoolean, ref string expression)
    {
      return evalStack.SignaturesValidation(checkBoolean, ref expression);
    }

    protected virtual SignatureList DoCreateSignatures()
    {
      return new SignatureList();
    }

    protected virtual bool OnParsePatterns()
    {
      char c = Thumb(0);
      if ((int) c == (int) EvalStack.SingleQuote)
      {
        int num = evalStack.PushStringConstant(thumbOffset, activeStorage);
        if (num < 0)
          RaiseError(296);
        thumbOffset += num;
        return true;
      }
      if ((int) c == (int) EvalStack.FloatPunctuation || char.IsNumber(c))
      {
        int num = evalStack.PushNumericConstant(thumbOffset);
        if (num > 0)
        {
          thumbOffset += num;
          return true;
        }
      }
      return ParseSubset(signatures.PatternsEntry, signatures.OperatorsEntry);
    }

    protected virtual bool OnParseOperands()
    {
      Signature signature1 = (Signature) null;
      int num1 = 0;
      int num2 = 0;
      int num3 = 0;
      int operatorsEntry = signatures.OperatorsEntry;
      for (int count = signatures.Count; operatorsEntry < count; ++operatorsEntry)
      {
        Signature signature2 = signatures[operatorsEntry];
        int num4 = signature2.IncludedName(evalStack.Expression, thumbOffset, activeStorage.Culture);
        int nameLen = signature2.NameLen;
        if (num4 > num1 || num4 == num1 && nameLen < num2)
        {
          num1 = num4;
          num2 = nameLen;
          signature1 = signature2;
          num3 = operatorsEntry;
        }
      }
      if (num1 == 0)
        return false;
      thumbOffset += num1;
      if (exactEqual && num3 == signatures.LOGICAL_COMPARE)
        signature1 = signatures[num3 + signatures.ExactEqualDifference];
      evalStack.PushCollector(signature1);
      return true;
    }

    internal EvalStack Compile(string expression, DataStorage activeStorage, bool checkBoolean)
    {
      return Compile(expression, activeStorage, false, checkBoolean, activeStorage.CaseSensitive, (EvalStack) null);
    }

    internal EvalStack Compile(string expression, DataStorage activeStorage, bool exactEqual, bool checkBoolean, bool caseSensitive, EvalStack evaluator)
    {
      if (expression == null || expression.Length == 0)
        RaiseError(280);
      this.activeStorage = activeStorage;
      try
      {
        evalStack = evaluator == null ? CreateEvalStackInstance() : evaluator;
        evalStack.Prepare();
        evalStack.Signatures = signatures;
      }
      catch (Exception ex)
      {
        evalStack = (EvalStack) null;
        RaiseError(ex, 281, expression);
      }
      this.exactEqual = exactEqual;
      try
      {
        evalStack.SaveExpression(expression);
        thumbOffset = 0;
        int delimitersCount = 0;
        if (!ParseExpression(ref delimitersCount, activeStorage, (char[]) null, ' ', (char[]) null))
        {
          if (evalStack != null && evalStack.IsEmptyCollector())
            RaiseError((Exception) null, 297, expression);
          else
            RaiseError((Exception) null, 298, (string) null);
        }
        if (evalStack.IsEmptyPcode())
          RaiseError((Exception) null, 285, expression);
        if (!evalStack.IsEmptyCollector())
          RaiseError((Exception) null, 297, expression);
        if (thumbOffset != evalStack.Expression.Length)
          RaiseError((Exception) null, 285, expression.Substring(thumbOffset, evalStack.Expression.Length - thumbOffset));
        evalStack.PushBreak();
        evalStack.PushPCode(evalStack.PopCollector());
        int error = SignaturesValidation(checkBoolean, ref expression);
        if (error > 0)
        {
          thumbOffset = 0;
          RaiseError((Exception) null, error, expression);
        }
        return evalStack;
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 285, expression);
      }
      finally
      {
        evalStack = (EvalStack) null;
      }
    }

    private EvalStack CreateEvalStackInstance()
    {
      return OnCreateEvalStackInstance(connection, activeStorage);
    }

    protected virtual EvalStack OnCreateEvalStackInstance(DirectConnection connection, DataStorage activeStorage)
    {
      return new EvalStack((Connection) connection, activeStorage);
    }

    public void Dispose()
    {
      if (isDisposed)
        return;
      isDisposed = true;
      GC.SuppressFinalize((object) this);
      if (signatures != null)
      {
        signatures.Clear();
        signatures = (SignatureList) null;
      }
      activeStorage = (DataStorage) null;
      evalStack = (EvalStack) null;
      connection = (DirectConnection) null;
    }
  }
}
