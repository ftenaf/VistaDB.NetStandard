using System;

namespace VistaDB.DDA
{
  public interface IVistaDBColumnAttributes : IVistaDBColumn, IVistaDBValue
  {
    new string Name { get; }

    string Description { get; set; }

    [Obsolete("Caption will be removed from a future build.  Use Description instead.", false)]
    string Caption { get; set; }

    int CodePage { get; }

    bool Encrypted { get; }

    bool Packed { get; }

    int RowIndex { get; }

    bool ExtendedType { get; }

    bool FixedType { get; }

    bool IsSystem { get; }

    IVistaDBColumnAttributesDifference Compare(IVistaDBColumnAttributes attributes);

    int UniqueId { get; }
  }
}
