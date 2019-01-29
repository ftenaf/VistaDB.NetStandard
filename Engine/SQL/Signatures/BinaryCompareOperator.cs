using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal abstract class BinaryCompareOperator : BinaryOperator
  {
    protected bool checkAll = true;
    protected VistaDBType operandType;
    protected bool rightOperandIsSubQuery;

    public BinaryCompareOperator(Signature leftOperand, SQLParser parser)
      : base(leftOperand, parser, 2)
    {
      this.dataType = VistaDBType.Bit;
      this.operandType = VistaDBType.Bit;
    }

    public override void SwitchToTempTable(SourceRow sourceRow, int columnIndex, SelectStatement.ResultColumn resultColumn)
    {
      if (this.leftOperand.Equals((object) resultColumn.Signature))
        this.leftOperand.SwitchToTempTable(sourceRow, columnIndex);
      if (!this.rightOperand.Equals((object) resultColumn.Signature))
        return;
      this.rightOperand.SwitchToTempTable(sourceRow, columnIndex);
    }

    protected override void DoParseRightOperand(SQLParser parser, int priority)
    {
      parser.SkipToken(true);
      if (parser.IsToken("ALL"))
      {
        parser.SkipToken(true);
        this.rightOperandIsSubQuery = true;
      }
      else if (parser.IsToken("SOME") || parser.IsToken("ANY"))
      {
        parser.SkipToken(true);
        this.rightOperandIsSubQuery = true;
        this.checkAll = false;
      }
      this.rightOperand = parser.NextSignature(false, true, 2);
      if (this.rightOperandIsSubQuery)
      {
        if (!(this.rightOperand is SubQuerySignature))
          throw new VistaDBSQLException(507, "subquery", this.rightOperand.LineNo, this.rightOperand.SymbolNo);
      }
      else
      {
        if (!(this.rightOperand is SubQuerySignature))
          return;
        this.rightOperandIsSubQuery = true;
      }
    }

    protected override IColumn InternalExecute()
    {
      if (!this.GetIsChanged())
        return this.result;
      ((IValue) this.result).Value = (object) null;
      this.needsEvaluation = false;
      if (this.leftOperand.AlwaysNull || this.rightOperand.AlwaysNull)
        return this.result;
      IColumn column1 = this.leftOperand.Execute();
      if (column1.IsNull)
        return this.result;
      if (this.rightOperandIsSubQuery)
      {
        this.leftValue = column1;
      }
      else
      {
        IColumn column2 = this.rightOperand.Execute();
        if (column2.IsNull)
          return this.result;
        this.Convert((IValue) column1, (IValue) this.leftValue);
        this.Convert((IValue) column2, (IValue) this.rightValue);
      }
      if (Utils.IsCharacterDataType(this.operandType))
      {
        ((IValue) this.leftValue).Value = (object) ((string) ((IValue) this.leftValue).Value).TrimEnd();
        if (!this.rightOperandIsSubQuery)
          ((IValue) this.rightValue).Value = (object) ((string) ((IValue) this.rightValue).Value).TrimEnd();
      }
      ((IValue) this.result).Value = (object) this.CompareOperands();
      return this.result;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = ConstantSignature.PrepareBinaryOperator(ref this.leftOperand, ref this.rightOperand, out this.operandType, false, true, this.text, this.lineNo, this.symbolNo);
      this.leftValue = this.CreateColumn(this.operandType);
      this.rightValue = this.CreateColumn(this.operandType);
      if (this.leftOperand.SignatureType == SignatureType.Column && this.rightOperand.SignatureType == SignatureType.Column)
        ((ColumnSignature) this.leftOperand).Table.AddJoinedTable(((ColumnSignature) this.rightOperand).Table);
      return signatureType;
    }

    protected override bool OnOptimize(ConstraintOperations constrainOperations)
    {
      return constrainOperations.AddLogicalCompare(this.leftOperand, this.rightOperand, this.GetCompareOperation(), this.GetRevCompareOperation(), false);
    }

    public override bool IsNull
    {
      get
      {
        if (!this.leftOperand.IsNull)
          return this.rightOperand.IsNull;
        return true;
      }
    }

    protected abstract bool CompareOperands();

    protected abstract CompareOperation GetCompareOperation();

    protected abstract CompareOperation GetRevCompareOperation();

    protected void CalcOptimizeLevel()
    {
      if (!this.leftOperand.Optimizable || !this.rightOperand.Optimizable)
      {
        this.optimizable = false;
      }
      else
      {
        switch (this.leftOperand.SignatureType)
        {
          case SignatureType.Constant:
          case SignatureType.Column:
          case SignatureType.Parameter:
          case SignatureType.ExternalColumn:
            switch (this.rightOperand.SignatureType)
            {
              case SignatureType.Constant:
              case SignatureType.Column:
              case SignatureType.Parameter:
              case SignatureType.ExternalColumn:
                this.optimizable = true;
                return;
              default:
                this.optimizable = false;
                return;
            }
          default:
            this.optimizable = false;
            break;
        }
      }
    }
  }
}
