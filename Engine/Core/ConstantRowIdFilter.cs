using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core
{
  internal class ConstantRowIdFilter : RowIdFilter
  {
    private Triangular.Value constant;

    internal ConstantRowIdFilter(Triangular.Value constant)
    {
      this.constant = constant;
    }

    protected override long OnConjunction(IOptimizedFilter filter)
    {
      if (!filter.IsConstant())
        return base.OnConjunction(filter);
      constant = Triangular.And(constant, ((ConstantRowIdFilter) filter).constant);
      return -1;
    }

    protected override long OnDisjunction(IOptimizedFilter filter)
    {
      if (!filter.IsConstant())
        return base.OnDisjunction(filter);
      constant = Triangular.Or(constant, ((ConstantRowIdFilter) filter).constant);
      return -1;
    }

    protected override long OnInvert(bool instant)
    {
      constant = Triangular.Not(constant);
      return -1;
    }

    protected override void OnSetRowStatus(Row row, bool valid)
    {
    }

    protected override bool OnGetValidRowStatus(Row row)
    {
      return constant == Triangular.Value.True;
    }

    public override bool IsConstant()
    {
      return true;
    }

    internal Triangular.Value Constant
    {
      get
      {
        return constant;
      }
    }
  }
}
