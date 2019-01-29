namespace VistaDB.DDA
{
  public interface IVistaDBColumn : IVistaDBValue
  {
    string Name { get; }

    int MaxLength { get; }

    object MinValue { get; }

    object MaxValue { get; }

    bool AllowNull { get; }

    bool ReadOnly { get; }

    int Compare(IVistaDBColumn column);

    bool Modified { get; }
  }
}
