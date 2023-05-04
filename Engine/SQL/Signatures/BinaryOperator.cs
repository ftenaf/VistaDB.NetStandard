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
      signatureType = SignatureType.Expression;
      DoParseRightOperand(parser, priority);
    }

    protected virtual void DoParseRightOperand(SQLParser parser, int priority)
    {
      rightOperand = parser.NextSignature(true, true, priority);
    }

    protected override bool IsEquals(Signature signature)
    {
      if (GetType() == signature.GetType() && leftOperand == ((BinaryOperator) signature).leftOperand)
        return rightOperand == ((BinaryOperator) signature).rightOperand;
      return false;
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
      leftOperand = leftOperand.Relink(signature, ref columnCount);
      rightOperand = rightOperand.Relink(signature, ref columnCount);
    }

    public override void SetChanged()
    {
      needsEvaluation = true;
      leftOperand.SetChanged();
      rightOperand.SetChanged();
    }

    public override void ClearChanged()
    {
      needsEvaluation = false;
      leftOperand.ClearChanged();
      rightOperand.ClearChanged();
    }

    protected override bool InternalGetIsChanged()
    {
      if (!needsEvaluation && !leftOperand.GetIsChanged())
        return rightOperand.GetIsChanged();
      return true;
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
      leftOperand.GetAggregateFunctions(list);
      rightOperand.GetAggregateFunctions(list);
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      bool distinct1;
      bool distinct2;
      bool flag = leftOperand.HasAggregateFunction(out distinct1) | rightOperand.HasAggregateFunction(out distinct2);
      distinct = distinct1 || distinct2;
      return flag;
    }

    public override bool AlwaysNull
    {
      get
      {
        if (!leftOperand.AlwaysNull)
          return rightOperand.AlwaysNull;
        return true;
      }
    }

    public override int ColumnCount
    {
      get
      {
        return leftOperand.ColumnCount + rightOperand.ColumnCount;
      }
    }
  }
}
