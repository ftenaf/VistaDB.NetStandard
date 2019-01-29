using System.Collections.Generic;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class BetweenOperator : Signature
  {
    protected Signature expression;
    protected Signature beginExpression;
    protected Signature endExpression;
    private IColumn beginValue;
    private IColumn endValue;
    private IColumn expValue1;
    private IColumn expValue2;

    public BetweenOperator(Signature expression, SQLParser parser)
      : base(parser)
    {
      this.signatureType = SignatureType.Expression;
      this.dataType = VistaDBType.Bit;
      this.expression = expression;
      this.beginExpression = parser.NextSignature(true, true, 2);
      parser.ExpectedExpression("AND");
      this.endExpression = parser.NextSignature(true, true, 2);
      this.beginValue = (IColumn) null;
      this.endValue = (IColumn) null;
    }

    public override SignatureType OnPrepare()
    {
      this.expression = ConstantSignature.PrepareAndCheckConstant(this.expression, VistaDBType.Unknown);
      this.beginExpression = ConstantSignature.PrepareAndCheckConstant(this.beginExpression, VistaDBType.Unknown);
      this.endExpression = ConstantSignature.PrepareAndCheckConstant(this.endExpression, VistaDBType.Unknown);
      VistaDBType maxDataType1 = Utils.GetMaxDataType(this.expression.DataType, this.beginExpression.DataType);
      VistaDBType maxDataType2 = Utils.GetMaxDataType(this.expression.DataType, this.endExpression.DataType);
      if (!Utils.CompatibleTypes(this.expression.DataType, maxDataType1) || !Utils.CompatibleTypes(this.expression.DataType, maxDataType2) || (!Utils.CompatibleTypes(this.beginExpression.DataType, maxDataType1) || !Utils.CompatibleTypes(this.endExpression.DataType, maxDataType2)))
        throw new VistaDBSQLException(558, "BETWEEN", this.lineNo, this.symbolNo);
      this.CalcOptimizeLevel();
      this.beginValue = this.CreateColumn(maxDataType1);
      this.endValue = this.CreateColumn(maxDataType2);
      this.expValue1 = this.CreateColumn(maxDataType1);
      this.expValue2 = maxDataType1 == maxDataType2 ? this.expValue1 : this.CreateColumn(maxDataType2);
      if ((this.expression.SignatureType == SignatureType.Constant || this.expression.AlwaysNull) && (this.beginExpression.SignatureType == SignatureType.Constant || this.beginExpression.AlwaysNull) && (this.endExpression.SignatureType == SignatureType.Constant || this.endExpression.AlwaysNull))
        return SignatureType.Constant;
      return this.signatureType;
    }

    private void CalcOptimizeLevel()
    {
      if (!this.expression.Optimizable || !this.beginExpression.Optimizable || !this.endExpression.Optimizable)
      {
        this.optimizable = false;
      }
      else
      {
        switch (this.expression.SignatureType)
        {
          case SignatureType.Constant:
          case SignatureType.Column:
          case SignatureType.Parameter:
          case SignatureType.ExternalColumn:
            switch (this.beginExpression.SignatureType)
            {
              case SignatureType.Constant:
              case SignatureType.Column:
              case SignatureType.Parameter:
              case SignatureType.ExternalColumn:
                switch (this.endExpression.SignatureType)
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
                return;
            }
          default:
            this.optimizable = false;
            break;
        }
      }
    }

    protected override bool OnOptimize(ConstraintOperations constrainOperations)
    {
      if (this.expression.SignatureType == SignatureType.Column)
        return constrainOperations.AddLogicalBetween((ColumnSignature) this.expression, this.beginExpression, this.endExpression, false);
      return false;
    }

    protected override IColumn InternalExecute()
    {
      if (this.GetIsChanged())
      {
        IColumn column1 = this.expression.Execute();
        IColumn column2 = this.beginExpression.Execute();
        IColumn column3 = this.endExpression.Execute();
        bool flag = !column1.IsNull && !column2.IsNull && !column3.IsNull;
        if (flag)
        {
          this.Convert((IValue) column2, (IValue) this.beginValue);
          this.Convert((IValue) column3, (IValue) this.endValue);
          this.Convert((IValue) column1, (IValue) this.expValue1);
          if (!object.ReferenceEquals((object) this.expValue1, (object) this.expValue2))
            this.Convert((IValue) column1, (IValue) this.expValue2);
          flag = this.ProcessResult(this.expValue1.Compare((IVistaDBColumn) this.beginValue) >= 0 && this.expValue2.Compare((IVistaDBColumn) this.endValue) <= 0);
        }
        ((IValue) this.result).Value = (object) flag;
      }
      return this.result;
    }

    protected virtual bool ProcessResult(bool result)
    {
      return result;
    }

    protected override bool IsEquals(Signature signature)
    {
      if (signature.GetType() != this.GetType())
        return false;
      BetweenOperator betweenOperator = (BetweenOperator) signature;
      if (this.expression == betweenOperator.expression && this.beginExpression == betweenOperator.beginExpression)
        return this.endExpression == betweenOperator.endExpression;
      return false;
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
      this.expression = this.expression.Relink(signature, ref columnCount);
      this.beginExpression = this.beginExpression.Relink(signature, ref columnCount);
      this.endExpression = this.endExpression.Relink(signature, ref columnCount);
    }

    public override void SetChanged()
    {
      this.expression.SetChanged();
      this.beginExpression.SetChanged();
      this.endExpression.SetChanged();
    }

    public override void ClearChanged()
    {
      this.expression.ClearChanged();
      this.beginExpression.ClearChanged();
      this.endExpression.ClearChanged();
    }

    protected override bool InternalGetIsChanged()
    {
      if (!this.expression.GetIsChanged() && !this.beginExpression.GetIsChanged())
        return this.endExpression.GetIsChanged();
      return true;
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
      this.expression.GetAggregateFunctions(list);
      this.beginExpression.GetAggregateFunctions(list);
      this.endExpression.GetAggregateFunctions(list);
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      bool distinct1;
      bool distinct2;
      bool distinct3;
      bool flag = this.expression.HasAggregateFunction(out distinct1) | this.beginExpression.HasAggregateFunction(out distinct2) | this.endExpression.HasAggregateFunction(out distinct3);
      distinct = distinct1 || distinct2 || distinct3;
      return flag;
    }

    public override int ColumnCount
    {
      get
      {
        return this.expression.ColumnCount + this.beginExpression.ColumnCount + this.endExpression.ColumnCount;
      }
    }

    public override bool AlwaysNull
    {
      get
      {
        if (!this.expression.AlwaysNull && !this.beginExpression.AlwaysNull)
          return this.endExpression.AlwaysNull;
        return true;
      }
    }

    public override bool IsNull
    {
      get
      {
        if (!this.expression.IsNull && !this.beginExpression.IsNull)
          return this.endExpression.IsNull;
        return true;
      }
    }
  }
}
