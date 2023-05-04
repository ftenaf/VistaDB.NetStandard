namespace VistaDB.Engine.Core
{
  internal class Timestamp : BigIntColumn
  {
    internal Timestamp()
    {
      ResetType(VistaDBType.Timestamp);
    }

    internal Timestamp(long val)
      : base(val)
    {
      ResetType(VistaDBType.Timestamp);
    }

    internal Timestamp(Timestamp col)
      : base(col)
    {
    }

    public override object Value
    {
      set
      {
        base.Value = value;
      }
    }

    public override VistaDBType InternalType
    {
      get
      {
        return VistaDBType.BigInt;
      }
    }

    internal override void AssignAttributes(string name, bool allowNull, bool readOnly, bool encrypted, bool packed)
    {
      allowNull = false;
      readOnly = true;
      base.AssignAttributes(name, allowNull, readOnly, encrypted, packed);
    }

    internal override void AssignAttributes(string name, bool allowNull, bool readOnly, bool encrypted, bool packed, string caption, string description)
    {
      allowNull = false;
      readOnly = true;
      base.AssignAttributes(name, allowNull, readOnly, encrypted, packed, caption, description);
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return new Timestamp(this);
    }
  }
}
