using System;
using System.Collections.Generic;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class EvalStack
  {
    internal static readonly char SingleQuote = '\'';
    internal static readonly char FloatPunctuation = '.';
    internal static readonly char FloatExponent = 'e';
    internal static readonly char FloatUpperExponent = 'E';
    internal static readonly char FloatMinusSign = '-';
    internal static readonly char FloatPlusSign = '+';
    internal static readonly char DecimalPunctuation = 'm';
    private ProcedureCode pcode;
    private Collector collector;
    private Connection connection;
    private DataStorage activeStorage;
    private char[] expression;
    private SignatureList signatures;
    protected Row rowResult;
    private Row.Column columnResult;

    internal EvalStack(Connection connection, DataStorage activeStorage)
    {
      this.pcode = new ProcedureCode();
      this.collector = new Collector();
      this.connection = connection;
      this.activeStorage = activeStorage;
    }

    internal char[] Expression
    {
      get
      {
        return this.expression;
      }
    }

    internal DataStorage ActiveStorage
    {
      get
      {
        return this.activeStorage;
      }
    }

    internal Row EvaluatedRow
    {
      get
      {
        return this.rowResult;
      }
    }

    internal Row.Column EvaluatedColumn
    {
      get
      {
        return this.columnResult;
      }
    }

    internal bool TrueBooleanValue
    {
      get
      {
        if (this.columnResult != (Row.Column) null && this.columnResult.Type == VistaDBType.Bit)
          return (bool) this.columnResult.Value;
        return false;
      }
    }

    internal VistaDBType EvaluatedType
    {
      get
      {
        return this.collector[0].Signature.ReturnType;
      }
    }

    internal int CodeLength
    {
      get
      {
        return this.pcode.Count;
      }
    }

    internal SignatureList Signatures
    {
      set
      {
        this.signatures = value;
      }
    }

    internal Row.Column FirstColumn
    {
      get
      {
        foreach (PCodeUnit pcodeUnit in (List<PCodeUnit>) this.pcode)
        {
          if (pcodeUnit.Signature.Group == this.signatures.COLUMN)
            return pcodeUnit.ResultColumn;
        }
        return (Row.Column) null;
      }
    }

    internal bool IsConstantResult
    {
      get
      {
        return false;
      }
    }

    private bool GoHead()
    {
      try
      {
        return this.pcode.GoHead();
      }
      finally
      {
        this.collector.GoHead();
      }
    }

    private PCodeUnit NextPCode()
    {
      return this.pcode.PopBack();
    }

    private PCodeUnit PrevPCode()
    {
      return this.pcode.Pop();
    }

    private PCodeUnit PeekPCode()
    {
      return this.pcode.Peek();
    }

    private int SyncNext()
    {
      PCodeUnit pcodeUnit1 = this.NextPCode();
      PCodeUnit unit = this.collector.ActivateNextRegister(pcodeUnit1);
      Signature signature1 = pcodeUnit1.Signature;
      int group = signature1.Group;
      Signature.Operations operation = signature1.Operation;
      int entry = signature1.Entry;
      int activeUnits1 = this.collector.ActiveUnits;
      int position = activeUnits1;
      if (group == this.signatures.BREAK)
        return this.TestBranches(0, activeUnits1 - 1);
      int num1;
      if (operation == Signature.Operations.BgnGroup)
      {
        num1 = 0;
        for (; position >= 0; --position)
        {
          Signature signature2 = this.collector[position].Signature;
          if (signature2.Operation == Signature.Operations.EndGroup && signature2.Entry == signature1.EndOfGroupEntry)
          {
            num1 = activeUnits1 - position - 1;
            this.collector.ExtractPosition(position);
            this.ExtractFirstSignature(signature2.Group, Signature.Operations.EndGroup);
            if (group == this.signatures.PARENTHESIS)
            {
              this.ExtractFirstSignature(group, Signature.Operations.BgnGroup);
              this.collector.Pop();
              return this.collector.ActiveUnits;
            }
            activeUnits1 = this.collector.ActiveUnits;
            break;
          }
        }
        if (position < 0)
          return -286;
        if (num1 - pcodeUnit1.DelimitersCount > 1)
          return -287;
        if (signature1.NumberFormalOperands != -1)
        {
          while (num1 != signature1.NumberFormalOperands && signature1.Group == group && signature1.Operation == operation)
            signature1 = this.signatures[++entry];
        }
        if (signature1.Group != group || signature1.Operation != operation)
          return -288;
      }
      else
        num1 = signature1.NumberFormalOperands;
      if (signature1.Operation != Signature.Operations.EndGroup)
      {
        if (num1 > 0)
        {
          while (signature1.Group == group && signature1.Operation == operation)
          {
            num1 = signature1.NumberFormalOperands;
            bool flag = true;
            int index = activeUnits1 - num1;
            if (index < 0)
            {
              if (signature1.NumberFormalOperands <= 0)
                return -285;
              signature1 = this.signatures[++entry];
            }
            else
            {
              int parameterIndex = 0;
              int offset = index;
              while (flag && parameterIndex < num1)
              {
                PCodeUnit pcodeUnit2 = this.collector[index];
                flag = signature1.CompatibleType(parameterIndex, pcodeUnit2.Signature.ReturnType);
                ++parameterIndex;
                ++index;
              }
              if (flag && parameterIndex == num1)
              {
                signature1 = signature1.DoCloneSignature();
                signature1.FixReturnTypeAndParameters(this.collector, offset);
                break;
              }
              signature1 = this.signatures[++entry];
            }
          }
          if (signature1.Group != group || signature1.Operation != operation)
            return -289;
        }
        if (num1 == 0)
        {
          while (signature1.Group == group && signature1.Operation == operation && (!(unit.ResultColumn == (Row.Column) null) && unit.ResultColumn.InternalType != signature1.ReturnType))
            signature1 = this.signatures[++entry];
          if (signature1.Group != group || signature1.Operation != operation)
            return -290;
        }
      }
      this.collector.MovePeekBy(-num1);
      unit.ParametersCount = num1;
      unit.Signature = signature1;
      int num2 = num1;
      for (int activeUnits2 = this.collector.ActiveUnits; activeUnits2 < this.collector.ActiveUnits + num1; ++activeUnits2)
        num2 += this.collector[activeUnits2].Depth;
      unit.Depth = num2;
      pcodeUnit1.CopyFrom(unit);
      this.collector.Peek().Signature = unit.Signature;
      return this.collector.ActiveUnits;
    }

    private void ExtractFirstSignature(int groupId, Signature.Operations operation)
    {
      for (int activeUnits = this.pcode.ActiveUnits; activeUnits >= 0; --activeUnits)
      {
        Signature signature = this.pcode[activeUnits].Signature;
        if (signature.Group == groupId && signature.Operation == operation)
        {
          this.pcode.ExtractPosition(activeUnits);
          break;
        }
      }
    }

    private int TestBranches(int beginCollectorPosition, int endCollectorPosition)
    {
      return beginCollectorPosition != endCollectorPosition ? -285 : -1;
    }

    private PCodeUnit ExecNext(Row contextRow)
    {
      PCodeUnit pcodeUnit = this.NextPCode();
      bool bypassNextGoup = false;
      int iterator = this.pcode.Iterator;
      this.collector.ExecRegister(pcodeUnit, this.connection, this.activeStorage, contextRow, ref bypassNextGoup, this.rowResult);
      this.pcode.Iterator = iterator;
      return this.BypassGroup(pcodeUnit, bypassNextGoup);
    }

    private PCodeUnit BypassGroup(PCodeUnit unit, bool bypassGroup)
    {
      Signature signature1 = unit.Signature;
      bypassGroup = bypassGroup && signature1.Group != this.signatures.BREAK;
      if (!bypassGroup)
        return unit;
      int bypassToGroup = signature1.BypassToGroup;
      int num = -1;
      Signature signature2;
      do
      {
        unit = this.NextPCode();
        signature2 = unit.Signature;
        ++num;
      }
      while (signature2.Group != this.signatures.BREAK && (unit.Depth != num || signature2.Operation != Signature.Operations.BgnGroup || signature2.Group != bypassToGroup));
      return unit;
    }

    internal void Prepare()
    {
      this.pcode.ClearCode();
      this.collector.ClearCode();
    }

    internal void SaveExpression(string expression)
    {
      this.expression = new char[expression.Length];
      expression.CopyTo(0, this.expression, 0, this.expression.Length);
    }

    internal void PushCollector(Signature signature)
    {
      this.collector.Push(new PCodeUnit(signature));
    }

    internal void PushCollector(PCodeUnit unit)
    {
      this.collector.Push(unit);
    }

    internal PCodeUnit PopCollector()
    {
      return this.collector.PopAndFree();
    }

    internal PCodeUnit PeekCollector()
    {
      return this.collector.Peek();
    }

    internal void PushBreak()
    {
      this.PushCollector(this.signatures[this.signatures.BREAK]);
    }

    internal void PushColumn(DataStorage storage, Row.Column sourceColumn)
    {
      this.PushCollector(this.signatures[this.signatures.COLUMN]);
      PCodeUnit pcodeUnit = this.PeekCollector();
      pcodeUnit.ResultColumn = sourceColumn;
      pcodeUnit.ActiveStorage = storage;
    }

    internal int PushStringConstant(int offset, DataStorage activeStorage)
    {
      int length = this.Expression.Length;
      char singleQuote = EvalStack.SingleQuote;
      string str = string.Empty;
      int num = 0;
      string val;
      while (true)
      {
        int startIndex = ++offset;
        bool flag = false;
        while (offset < length && (flag = singleQuote.CompareTo(this.Expression[offset]) != 0))
          ++offset;
        if (offset < length || !flag)
        {
          val = str + new string(this.Expression, startIndex, offset - startIndex);
          ++offset;
          if (offset < length && singleQuote.CompareTo(this.Expression[offset]) == 0)
          {
            ++num;
            str = val + (object) singleQuote;
          }
          else
            goto label_8;
        }
        else
          break;
      }
      return -1;
label_8:
      Constant constant = (Constant) this.signatures[this.signatures.CONSTANT].DoCloneSignature();
      this.PushCollector((Signature) constant);
      PCodeUnit pcodeUnit = this.PeekCollector();
      NCharColumn ncharColumn = new NCharColumn(val, 8192, activeStorage.Culture, true, NCharColumn.DefaultUnicode);
      constant.ConstantColumn = (Row.Column) ncharColumn;
      pcodeUnit.ResultColumn = (Row.Column) ncharColumn;
      pcodeUnit.ActiveStorage = (DataStorage) null;
      return val.Length + 2 + num;
    }

    internal int PushNumericConstant(int offset)
    {
      int length1 = this.Expression.Length;
      int startIndex = offset;
      while (offset < length1 && char.IsNumber(this.Expression[offset]))
        ++offset;
      bool flag1 = offset < length1 && (int) this.Expression[offset] == (int) EvalStack.FloatPunctuation;
      if (flag1)
      {
        ++offset;
        while (offset < length1 && char.IsNumber(this.Expression[offset]))
          ++offset;
      }
      bool flag2 = offset < length1 && (int) this.Expression[offset] == (int) EvalStack.DecimalPunctuation;
      bool flag3 = offset < length1 && ((int) this.Expression[offset] == (int) EvalStack.FloatExponent || (int) this.Expression[offset] == (int) EvalStack.FloatUpperExponent);
      if (flag3)
      {
        if (++offset < length1 && ((int) this.Expression[offset] == (int) EvalStack.FloatPlusSign || (int) this.Expression[offset] == (int) EvalStack.FloatMinusSign))
          ++offset;
        int num = offset;
        while (offset < length1 && char.IsNumber(this.Expression[offset]))
          ++offset;
        if (num == offset)
          throw new VistaDBException(299, new string(this.Expression, startIndex, offset));
      }
      else if (flag2)
        ++offset;
      Constant constant = (Constant) this.signatures[this.signatures.CONSTANT].DoCloneSignature();
      this.PushCollector((Signature) constant);
      PCodeUnit pcodeUnit = this.PeekCollector();
      int length2 = offset - startIndex;
      if (flag2)
      {
        string str = new string(this.Expression, startIndex, length2);
        Decimal val = Decimal.Parse(new string(this.Expression, startIndex, length2 - 1));
        pcodeUnit.ResultColumn = (Row.Column) new DecimalColumn(val);
      }
      else if (flag1 || flag3)
      {
        double val = double.Parse(new string(this.Expression, startIndex, length2), CrossConversion.NumberFormat);
        pcodeUnit.ResultColumn = (Row.Column) new FloatColumn(val);
      }
      else
      {
        long val = long.Parse(new string(this.Expression, startIndex, length2));
        pcodeUnit.ResultColumn = val <= (long) int.MaxValue ? (val <= (long) short.MaxValue ? (Row.Column) new SmallIntColumn((short) val) : (Row.Column) new IntColumn((int) val)) : (Row.Column) new BigIntColumn(val);
      }
      constant.ConstantColumn = pcodeUnit.ResultColumn;
      pcodeUnit.ActiveStorage = (DataStorage) null;
      return length2;
    }

    internal int PushRunTimeTableContext(int offset, int nameLen)
    {
      return -1;
    }

    internal bool IsEmptyCollector()
    {
      return this.collector.Empty;
    }

    internal void PushPCode(PCodeUnit unit)
    {
      this.pcode.Push(unit);
    }

    internal bool IsEmptyPcode()
    {
      return this.pcode.Empty;
    }

    internal void InsertEndOfGroup(int endOfGroupEntry)
    {
      this.PushCollector(this.signatures[endOfGroupEntry]);
      this.PushPCode(this.PopCollector());
    }

    internal void InsertElseGroup()
    {
    }

    internal void InsertIfGroupFinalization()
    {
    }

    internal void ReleaseCollector()
    {
      while (this.PeekCollector().Signature.Group != this.signatures.BREAK)
        this.PushPCode(this.PopCollector());
    }

    internal int SignaturesValidation(bool checkBoolean, ref string expression)
    {
      if (!this.GoHead())
        return 285;
      int num;
      do
      {
        num = this.SyncNext();
      }
      while (num >= 0);
      if (num < -1)
      {
        expression = new string(this.PeekPCode().Signature.Name);
        return -num;
      }
      return checkBoolean && this.EvaluatedType != VistaDBType.Bit ? 291 : 0;
    }

    internal void Exec(Row contextRow)
    {
      this.Exec(contextRow, (Row) null);
    }

    internal virtual void Exec(Row contextRow, Row targetResult)
    {
      this.rowResult = targetResult;
      if (this.rowResult != null)
      {
        this.rowResult.Clear();
        this.rowResult.CopyMetaData(contextRow);
      }
      if (!this.GoHead())
        throw new VistaDBException(308, new string(this.expression));
      do
        ;
      while (this.ExecNext(contextRow).Signature.Group != this.signatures.BREAK);
      this.columnResult = this.collector.ColumnResult;
      if (targetResult == null || targetResult.Count != 0 || !(this.columnResult != (Row.Column) null))
        return;
      targetResult.AppendColumn((IColumn) this.columnResult);
    }

    internal List<Row.Column> EnumColumns()
    {
      List<Row.Column> columnList = new List<Row.Column>();
      foreach (PCodeUnit pcodeUnit in (List<PCodeUnit>) this.pcode)
      {
        if (pcodeUnit.Signature.Group == this.signatures.COLUMN)
          columnList.Add(pcodeUnit.ResultColumn);
      }
      return columnList;
    }
  }
}
