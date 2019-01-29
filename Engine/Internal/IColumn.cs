using VistaDB.DDA;

namespace VistaDB.Engine.Internal
{
  internal interface IColumn : IVistaDBColumn, IValue, IVistaDBValue
  {
    int RowIndex { get; }

    bool Edited { get; }

    bool ExtendedType { get; }

    bool Descending { get; }

    bool IsSystem { get; }

    int CompareRank(IColumn b);

    IColumn Clone();
  }
}
