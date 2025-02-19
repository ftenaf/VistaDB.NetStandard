﻿using System.Globalization;

namespace VistaDB.Engine.Core
{
  internal class NTextColumn : TextColumn
  {
    internal NTextColumn(string val, CultureInfo culture, bool caseInsensitive, int codePage)
      : base(val, NCharColumn.MakeUpUnicodePage(codePage), culture, caseInsensitive)
    {
      ResetType(VistaDBType.NText);
    }

    internal NTextColumn(NTextColumn col)
      : base(col)
    {
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return new NTextColumn(this);
    }

    public override int MaxLength
    {
      get
      {
        return base.MaxLength / 2;
      }
    }
  }
}
