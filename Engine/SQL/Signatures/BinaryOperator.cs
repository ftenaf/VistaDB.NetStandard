using System.Collections.Generic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal abstract class BinaryOperator : Signature
  {
    protected bool needsEvaluation = true;
    protected Signature leftOperand;
    protected Signature rightOperand;
    protected IColumn leftValue;
    protected IColumn rightValue;

    public BinaryOperator(Signature leftOperand, SQLParser parser, int priority)
      : base(parser)
    {
      this.leftOperand = leftOperand;
      this.signatureType = SignatureType.Expression;
      this.DoParseRightOperand(parser, priority);
    }

    protected virtual void DoParseRightOperand(SQLParser parser, int priority)
    {
      this.rightOperand = parser.NextSignature(true, true, priority);
    }

    protected override bool IsEquals(Signature signature)
    {
      if (this.GetType() == signature.GetType() && this.leftOperand == ((BinaryOperator) signature).leftOperand)
        return this.rightOperand == ((BinaryOperator) signature).rightOperand;
      return false;
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
      this.leftOperand = this.leftOperand.Relink(signature, ref columnCount);
      this.rightOperand = this.rightOperand.Relink(signature, ref columnCount);
    }

    public override void SetChanged()
    {
      this.needsEvaluation = true;
      this.leftOperand.SetChanged();
      this.rightOperand.SetChanged();
    }

    public override void ClearChanged()
    {
      this.needsEvaluation = false;
      this.leftOperand.ClearChanged();
      this.rightOperand.ClearChanged();
    }

    protected override bool InternalGetIsChanged()
    {
      if (!this.needsEvaluation && !this.leftOperand.GetIsChanged())
        return this.rightOperand.GetIsChanged();
      return true;
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
      this.leftOperand.GetAggregateFunctions(list);
      this.rightOperand.GetAggregateFunctions(list);
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      bool distinct1;
      bool distinct2;
      bool flag = this.leftOperand.HasAggregateFunction(out distinct1) | this.rightOperand.HasAggregateFunction(out distinct2);
      distinct = distinct1 || distinct2;
      return flag;
    }

    public override bool AlwaysNull
    {
      get
      {
        if (!this.leftOperand.AlwaysNull)
          return this.rightOperand.AlwaysNull;
        return true;
      }
    }

    public override int ColumnCount
    {
      get
      {
        return this.leftOperand.ColumnCount + this.rightOperand.ColumnCount;
      }
    }
  }
}
