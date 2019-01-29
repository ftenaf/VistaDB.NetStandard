namespace VistaDB.Engine.Internal
{
  internal interface IOptimizedFilter
  {
    void Conjunction(IOptimizedFilter filter);

    void Disjunction(IOptimizedFilter filter);

    void Invert(bool instant);

    bool IsConstant();

    long RowCount { get; }
  }
}
