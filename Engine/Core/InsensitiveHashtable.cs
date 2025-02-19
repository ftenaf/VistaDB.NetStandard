﻿using System.Collections;
using System.Globalization;

namespace VistaDB.Engine.Core
{
  internal class InsensitiveHashtable : Hashtable
  {
    private CultureInfo culture = CultureInfo.InvariantCulture;

    public InsensitiveHashtable()
    {
    }

    public InsensitiveHashtable(int capacity)
      : base(capacity)
    {
    }

    public override bool ContainsKey(object key)
    {
      return base.ContainsKey(((string)key).ToUpper(culture));
    }

    protected override int GetHash(object key)
    {
      return base.GetHash(((string)key).ToUpper(culture));
    }

    protected override bool KeyEquals(object item, object key)
    {
      return base.KeyEquals(((string)item).ToUpper(culture), ((string)key).ToUpper(culture));
    }

    public override IDictionaryEnumerator GetEnumerator()
    {
      return (IDictionaryEnumerator) Values.GetEnumerator();
    }
  }
}
