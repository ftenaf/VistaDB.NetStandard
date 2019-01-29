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
      this.constant = Triangular.And(this.constant, ((ConstantRowIdFilter) filter).constant);
      return -1;
    }

    protected override long OnDisjunction(IOptimizedFilter filter)
    {
      if (!filter.IsConstant())
        return base.OnDisjunction(filter);
      this.constant = Triangular.Or(this.constant, ((ConstantRowIdFilter) filter).constant);
      return -1;
    }

    protected override long OnInvert(bool instant)
    {
      this.constant = Triangular.Not(this.constant);
      return -1;
    }

    protected override void OnSetRowStatus(Row row, bool valid)
    {
    }

    protected override bool OnGetValidRowStatus(Row row)
    {
      return this.constant == Triangular.Value.True;
    }

    public override bool IsConstant()
    {
      return true;
    }

    internal Triangular.Value Constant
    {
      get
      {
        return this.constant;
      }
    }
  }
}
