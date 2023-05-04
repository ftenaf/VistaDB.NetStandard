﻿using System.Collections;
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
        Add((object) index, (object) DataStorage.CreateRowColumn((VistaDBType) index, false, CultureInfo.InvariantCulture));
    }

    internal static int GetMaxLength(VistaDBType type)
    {
      if (!properties.Contains((object) type))
        return -1;
      return ((Row.Column)properties[(object) type]).MaxLength;
    }
  }
}
