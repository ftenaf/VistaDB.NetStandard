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
      this.signatureType = SignatureType.Expression;
      this.dataType = VistaDBType.Bit;
      this.expression = expression;
      this.optimizable = true;
      parser.SkipToken(true);
      if (parser.IsToken("NULL"))
      {
        this.isNull = true;
      }
      else
      {
        if (!parser.IsToken("NOT"))
          throw new VistaDBSQLException(560, parser.TokenValue.Token, this.lineNo, this.symbolNo);
        parser.SkipToken(true);
        parser.ExpectedExpression("NULL");
        this.isNull = false;
      }
      parser.SkipToken(false);
    }

    public override SignatureType OnPrepare()
    {
      this.expression = ConstantSignature.PrepareAndCheckConstant(this.expression, VistaDBType.Unknown);
      if (this.expression.AlwaysNull)
        return SignatureType.Constant;
      return this.signatureType;
    }

    protected override IColumn InternalExecute()
    {
      if (this.GetIsChanged())
        ((IValue) this.result).Value = (object) (this.expression.Execute().IsNull == this.isNull);
      return this.result;
    }

    protected override bool OnOptimize(ConstraintOperations constrainOperations)
    {
      return constrainOperations.AddLogicalIsNull(this.expression, this.isNull);
    }

    protected override bool IsEquals(Signature signature)
    {
      if (signature.GetType() != this.GetType())
        return false;
      IsNullOperator isNullOperator = (IsNullOperator) signature;
      if (this.expression == isNullOperator.expression)
        return this.isNull == isNullOperator.isNull;
      return false;
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
      this.expression = this.expression.Relink(signature, ref columnCount);
    }

    public override void SetChanged()
    {
      this.expression.SetChanged();
    }

    public override void ClearChanged()
    {
      this.expression.ClearChanged();
    }

    protected override bool InternalGetIsChanged()
    {
      return this.expression.GetIsChanged();
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
      this.expression.GetAggregateFunctions(list);
    }

    public override int ColumnCount
    {
      get
      {
        return this.expression.ColumnCount;
      }
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      return this.expression.HasAggregateFunction(out distinct);
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
