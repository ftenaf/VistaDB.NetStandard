using System.Collections.Generic;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class IsNullOperator : Signature
  {
    private Signature expression;
    private bool isNull;

    public IsNullOperator(Signature expression, SQLParser parser)
      : base(parser)
    {
      signatureType = SignatureType.Expression;
      dataType = VistaDBType.Bit;
      this.expression = expression;
      optimizable = true;
      parser.SkipToken(true);
      if (parser.IsToken("NULL"))
      {
        isNull = true;
      }
      else
      {
        if (!parser.IsToken("NOT"))
          throw new VistaDBSQLException(560, parser.TokenValue.Token, lineNo, symbolNo);
        parser.SkipToken(true);
        parser.ExpectedExpression("NULL");
        isNull = false;
      }
      parser.SkipToken(false);
    }

    public override SignatureType OnPrepare()
    {
      expression = ConstantSignature.PrepareAndCheckConstant(expression, VistaDBType.Unknown);
      if (expression.AlwaysNull)
        return SignatureType.Constant;
      return signatureType;
    }

    protected override IColumn InternalExecute()
    {
      if (GetIsChanged())
        ((IValue) result).Value = (object) (expression.Execute().IsNull == isNull);
      return result;
    }

    protected override bool OnOptimize(ConstraintOperations constrainOperations)
    {
      return constrainOperations.AddLogicalIsNull(expression, isNull);
    }

    protected override bool IsEquals(Signature signature)
    {
      if (signature.GetType() != GetType())
        return false;
      IsNullOperator isNullOperator = (IsNullOperator) signature;
      if (expression == isNullOperator.expression)
        return isNull == isNullOperator.isNull;
      return false;
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
      expression = expression.Relink(signature, ref columnCount);
    }

    public override void SetChanged()
    {
      expression.SetChanged();
    }

    public override void ClearChanged()
    {
      expression.ClearChanged();
    }

    protected override bool InternalGetIsChanged()
    {
      return expression.GetIsChanged();
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
      expression.GetAggregateFunctions(list);
    }

    public override int ColumnCount
    {
      get
      {
        return expression.ColumnCount;
      }
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      return expression.HasAggregateFunction(out distinct);
    }

    public override bool AlwaysNull
    {
      get
      {
        return false;
      }
    }
  }
}
