using System.Collections;
using System.Globalization;
using VistaDB.Engine.Core;

namespace VistaDB.Engine.Internal
{
  internal class ColumnsProperties : Hashtable
  {
    private static ColumnsProperties properties = new ColumnsProperties();

    private ColumnsProperties()
    {
      for (int index = 0; index < 32; ++index)
        this.Add((object) index, (object) DataStorage.CreateRowColumn((VistaDBType) index, false, CultureInfo.InvariantCulture));
    }

    internal static int GetMaxLength(VistaDBType type)
    {
      if (!ColumnsProperties.properties.Contains((object) type))
        return -1;
      return ((Row.Column) ColumnsProperties.properties[(object) type]).MaxLength;
    }
  }
}
