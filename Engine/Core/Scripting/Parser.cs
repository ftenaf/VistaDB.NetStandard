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
      this.signatures = this.DoCreateSignatures();
    }

    private void RaiseError(int error)
    {
      this.RaiseError((Exception) null, error, (string) null);
    }

    private void RaiseError(Exception ex, int error, string message)
    {
      try
      {
        throw this.evalStack == null ? new VistaDBException(ex, error, message) : new VistaDBException(ex, error, new string(this.evalStack.Expression).Substring(this.thumbOffset));
      }
      finally
      {
        if (this.evalStack != null)
          this.evalStack = (EvalStack) null;
      }
    }

    private bool ParseSubset(int from, int to)
    {
      for (int index = from; index < to; ++index)
      {
        Signature signature = this.signatures[index];
        int num = signature.IncludedName(this.evalStack.Expression, this.thumbOffset, this.activeStorage.Culture);
        if (num > 0)
        {
          this.evalStack.PushCollector(signature);
          this.thumbOffset += num;
          return true;
        }
      }
      return false;
    }

    private char Thumb(int offset)
    {
      return this.evalStack.Expression[this.thumbOffset + offset];
    }

    private int ExtractTo(char finalChar)
    {
      int thumbOffset = this.thumbOffset;
      int index = thumbOffset;
      for (int length = this.evalStack.Expression.Length; index < length; ++index)
      {
        if (finalChar.Equals(this.evalStack.Expression[index]))
          return index - thumbOffset + 1;
      }
      return 0;
    }

    private void BypassSpaces(char spaceSymbol)
    {
      int length = this.evalStack.Expression.Length;
      while (this.thumbOffset < length && spaceSymbol.CompareTo(this.Thumb(0)) >= 0)
        ++this.thumbOffset;
    }

    private bool EqualPattern(char[] source, int sourceOffset, char[] destination, int destinationOffset, int len)
    {
      while (sourceOffset < source.Length && destinationOffset < destination.Length && source[sourceOffset++].Equals(destination[destinationOffset++]))
        --len;
      return len == 0;
    }

    private void BypassComments(char[] endGroup)
    {
      int length = this.evalStack.Expression.Length;
      while (this.thumbOffset < length && !this.EqualPattern(this.evalStack.Expression, this.thumbOffset, endGroup, 0, endGroup.Length))
        ++this.thumbOffset;
    }

    private bool TestSquareBrackets(ref int expectedNameLength, ref int extraLen)
    {
      if (this.Thumb(0) != '[')
        return false;
      extraLen = 2;
      expectedNameLength = this.ExtractTo(']') - extraLen;
      if (expectedNameLength <= 0)
        this.RaiseError(286);
      return true;
    }

    private bool ParseCommentsAndColumns(char[] bgnOfGroup, char spaceSymbol, DataStorage activeStorage)
    {
      this.BypassSpaces(spaceSymbol);
      int length = this.evalStack.Expression.Length;
      int num = length - this.thumbOffset;
      if (bgnOfGroup != null)
      {
        if (num <= 0 || !this.EqualPattern(bgnOfGroup, 0, this.evalStack.Expression, this.thumbOffset, bgnOfGroup.Length))
          return false;
        this.thumbOffset += bgnOfGroup.Length;
        this.BypassSpaces(spaceSymbol);
        num = length - this.thumbOffset;
      }
      if (num == 0)
        return false;
      if (this.signatures[this.signatures.COMMENTS_INLINE].IncludedName(this.evalStack.Expression, this.thumbOffset, this.activeStorage.Culture) > 0)
      {
        this.BypassComments(Parser.endLine);
        num = length - this.thumbOffset;
      }
      if (num == 0)
        return false;
      Signature signature = this.signatures[this.signatures.COMMENTS];
      if (signature.IncludedName(this.evalStack.Expression, this.thumbOffset, this.activeStorage.Culture) > 0)
      {
        this.BypassComments(signature.EndOfGroup);
        num = length - this.thumbOffset;
      }
      if (num == 0 || this.activeStorage == null)
        return false;
      int expectedNameLength = 0;
      int extraLen = 0;
      Row.Column sourceColumn;
      if (this.TestSquareBrackets(ref expectedNameLength, ref extraLen))
      {
        sourceColumn = this.activeStorage.LookForColumn(new string(this.evalStack.Expression, this.thumbOffset + 1, expectedNameLength));
        if (sourceColumn == (Row.Column) null)
          return false;
      }
      else
      {
        sourceColumn = this.activeStorage.LookForColumn(this.evalStack.Expression, this.thumbOffset, false);
        if (sourceColumn == (Row.Column) null)
          return false;
        expectedNameLength = sourceColumn.Name.Length;
      }
      this.evalStack.PushColumn(activeStorage, sourceColumn);
      this.thumbOffset += expectedNameLength + extraLen;
      return true;
    }

    private bool ParsePatterns()
    {
      if (this.thumbOffset < this.evalStack.Expression.Length)
        return this.OnParsePatterns();
      return false;
    }

    private bool ParseOperands()
    {
      if (this.thumbOffset < this.evalStack.Expression.Length)
        return this.OnParseOperands();
      return false;
    }

    private bool ParseTableNames(DataStorage activeStorage, char spaceChar)
    {
      int length = this.evalStack.Expression.Length;
      if (length == this.thumbOffset)
        return false;
      this.BypassSpaces(spaceChar);
      if (length == this.thumbOffset)
        return false;
      int expectedNameLength = 0;
      int extraLen = 0;
      if (this.TestSquareBrackets(ref expectedNameLength, ref extraLen))
      {
        if (this.connection.LookForTable(new string(this.evalStack.Expression, this.thumbOffset + 1, expectedNameLength)) == null)
          return false;
      }
      else
      {
        DataStorage dataStorage = this.connection.LookForTable(this.evalStack.Expression, this.thumbOffset, false);
        if (dataStorage == null)
          return false;
        expectedNameLength = dataStorage.Name.Length;
      }
      this.thumbOffset += expectedNameLength + extraLen;
      return true;
    }

    private bool CheckIFClause(Signature signature, ref bool ifGroup, ref bool elseExpected)
    {
      if (signature.Group == this.signatures.IF)
        this.evalStack.InsertEndOfGroup(this.signatures.PARENTHESIS + 1);
      if (ifGroup)
      {
        if (signature.Group != this.signatures.THEN)
        {
          this.thumbOffset -= signature.Name.Length;
          this.RaiseError(293);
        }
        ifGroup = false;
        elseExpected = true;
        return false;
      }
      if (signature.Group == this.signatures.THEN)
      {
        this.thumbOffset -= signature.Name.Length;
        this.RaiseError(294);
      }
      if (signature.Group == this.signatures.ELSE)
      {
        if (!elseExpected)
        {
          this.thumbOffset -= signature.Name.Length;
          this.RaiseError(295);
        }
        elseExpected = false;
        return true;
      }
      if (elseExpected)
      {
        this.evalStack.InsertElseGroup();
        elseExpected = false;
      }
      return false;
    }

    private bool ParseExpression(ref int delimitersCount, DataStorage activeStorage, char[] bgnOfGroup, char spaceChar, char[] delimiter)
    {
      this.evalStack.PushBreak();
      bool flag1 = true;
      bool ifGroup = false;
      bool elseExpected = false;
      while (this.ParseCommentsAndColumns(bgnOfGroup, spaceChar, activeStorage) || this.ParsePatterns() || (this.ParseOperands() || this.ParseTableNames(activeStorage, spaceChar)))
      {
        bgnOfGroup = (char[]) null;
        PCodeUnit unit = this.evalStack.PopCollector();
        Signature signature1 = unit.Signature;
        switch (signature1.Operation)
        {
          case Signature.Operations.BgnGroup:
            bool flag2 = this.CheckIFClause(signature1, ref ifGroup, ref elseExpected);
            this.evalStack.InsertEndOfGroup(signature1.EndOfGroupEntry);
            int delimitersCount1 = 0;
            unit.ContentBgn = this.thumbOffset;
            if (!this.ParseExpression(ref delimitersCount1, unit.ActiveStorage == null ? activeStorage : unit.ActiveStorage, signature1.BgnOfGroup, signature1.SpaceChar, signature1.Delimiter))
              return false;
            unit.ContentEnd = this.thumbOffset;
            if (this.evalStack.PopCollector().Signature != this.signatures[signature1.EndOfGroupEntry])
              this.RaiseError(286);
            ifGroup = signature1.Group == this.signatures.IF && delimitersCount1 == 0;
            unit.DelimitersCount = delimitersCount1;
            this.evalStack.PushPCode(unit);
            if (flag2)
              this.evalStack.InsertIfGroupFinalization();
            flag1 = false;
            continue;
          case Signature.Operations.EndGroup:
            this.evalStack.ReleaseCollector();
            this.evalStack.PopCollector();
            this.evalStack.PushCollector(unit);
            return true;
          case Signature.Operations.Delimiter:
            if (!signature1.IsSameName(delimiter, 0, this.activeStorage.Culture))
              this.RaiseError((Exception) null, 287, new string(delimiter));
            ++delimitersCount;
            this.evalStack.ReleaseCollector();
            continue;
          default:
            Signature signature2 = this.evalStack.PeekCollector().Signature;
            if (flag1 && signature1.UnaryOverloading && signature2.AllowUnaryToFollow)
            {
              signature1 = this.signatures[signature1.UnaryEntry];
              unit.Signature = signature1;
            }
            for (; signature2.Priority >= signature1.Priority && signature2.Group != this.signatures.BREAK; signature2 = this.evalStack.PeekCollector().Signature)
              this.evalStack.PushPCode(this.evalStack.PopCollector());
            this.evalStack.PushCollector(unit);
            continue;
        }
      }
      if (this.evalStack == null)
        return false;
      if (ifGroup)
        this.RaiseError(293);
      if (elseExpected)
        this.evalStack.InsertElseGroup();
      this.evalStack.ReleaseCollector();
      this.evalStack.PopCollector();
      return true;
    }

    private int SignaturesValidation(bool checkBoolean, ref string expression)
    {
      return this.evalStack.SignaturesValidation(checkBoolean, ref expression);
    }

    protected virtual SignatureList DoCreateSignatures()
    {
      return new SignatureList();
    }

    protected virtual bool OnParsePatterns()
    {
      char c = this.Thumb(0);
      if ((int) c == (int) EvalStack.SingleQuote)
      {
        int num = this.evalStack.PushStringConstant(this.thumbOffset, this.activeStorage);
        if (num < 0)
          this.RaiseError(296);
        this.thumbOffset += num;
        return true;
      }
      if ((int) c == (int) EvalStack.FloatPunctuation || char.IsNumber(c))
      {
        int num = this.evalStack.PushNumericConstant(this.thumbOffset);
        if (num > 0)
        {
          this.thumbOffset += num;
          return true;
        }
      }
      return this.ParseSubset(this.signatures.PatternsEntry, this.signatures.OperatorsEntry);
    }

    protected virtual bool OnParseOperands()
    {
      Signature signature1 = (Signature) null;
      int num1 = 0;
      int num2 = 0;
      int num3 = 0;
      int operatorsEntry = this.signatures.OperatorsEntry;
      for (int count = this.signatures.Count; operatorsEntry < count; ++operatorsEntry)
      {
        Signature signature2 = this.signatures[operatorsEntry];
        int num4 = signature2.IncludedName(this.evalStack.Expression, this.thumbOffset, this.activeStorage.Culture);
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
      this.thumbOffset += num1;
      if (this.exactEqual && num3 == this.signatures.LOGICAL_COMPARE)
        signature1 = this.signatures[num3 + this.signatures.ExactEqualDifference];
      this.evalStack.PushCollector(signature1);
      return true;
    }

    internal EvalStack Compile(string expression, DataStorage activeStorage, bool checkBoolean)
    {
      return this.Compile(expression, activeStorage, false, checkBoolean, activeStorage.CaseSensitive, (EvalStack) null);
    }

    internal EvalStack Compile(string expression, DataStorage activeStorage, bool exactEqual, bool checkBoolean, bool caseSensitive, EvalStack evaluator)
    {
      if (expression == null || expression.Length == 0)
        this.RaiseError(280);
      this.activeStorage = activeStorage;
      try
      {
        this.evalStack = evaluator == null ? this.CreateEvalStackInstance() : evaluator;
        this.evalStack.Prepare();
        this.evalStack.Signatures = this.signatures;
      }
      catch (Exception ex)
      {
        this.evalStack = (EvalStack) null;
        this.RaiseError(ex, 281, expression);
      }
      this.exactEqual = exactEqual;
      try
      {
        this.evalStack.SaveExpression(expression);
        this.thumbOffset = 0;
        int delimitersCount = 0;
        if (!this.ParseExpression(ref delimitersCount, activeStorage, (char[]) null, ' ', (char[]) null))
        {
          if (this.evalStack != null && this.evalStack.IsEmptyCollector())
            this.RaiseError((Exception) null, 297, expression);
          else
            this.RaiseError((Exception) null, 298, (string) null);
        }
        if (this.evalStack.IsEmptyPcode())
          this.RaiseError((Exception) null, 285, expression);
        if (!this.evalStack.IsEmptyCollector())
          this.RaiseError((Exception) null, 297, expression);
        if (this.thumbOffset != this.evalStack.Expression.Length)
          this.RaiseError((Exception) null, 285, expression.Substring(this.thumbOffset, this.evalStack.Expression.Length - this.thumbOffset));
        this.evalStack.PushBreak();
        this.evalStack.PushPCode(this.evalStack.PopCollector());
        int error = this.SignaturesValidation(checkBoolean, ref expression);
        if (error > 0)
        {
          this.thumbOffset = 0;
          this.RaiseError((Exception) null, error, expression);
        }
        return this.evalStack;
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 285, expression);
      }
      finally
      {
        this.evalStack = (EvalStack) null;
      }
    }

    private EvalStack CreateEvalStackInstance()
    {
      return this.OnCreateEvalStackInstance(this.connection, this.activeStorage);
    }

    protected virtual EvalStack OnCreateEvalStackInstance(DirectConnection connection, DataStorage activeStorage)
    {
      return new EvalStack((Connection) connection, activeStorage);
    }

    public void Dispose()
    {
      if (this.isDisposed)
        return;
      this.isDisposed = true;
      GC.SuppressFinalize((object) this);
      if (this.signatures != null)
      {
        this.signatures.Clear();
        this.signatures = (SignatureList) null;
      }
      this.activeStorage = (DataStorage) null;
      this.evalStack = (EvalStack) null;
      this.connection = (DirectConnection) null;
    }
  }
}
