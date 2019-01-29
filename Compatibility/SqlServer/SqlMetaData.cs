namespace VistaDB.Compatibility.SqlServer
{
  public sealed class SqlMetaData
  {
    public SqlMetaData(string name, VistaDBType dbType)
      : this(name, dbType, true)
    {
    }

    public SqlMetaData(string name, VistaDBType dbType, bool allowNull)
    {
      this.Name = name;
      this.VistaDBType = dbType;
      this.AllowNull = allowNull;
    }

    public SqlMetaData(string name, VistaDBType dbType, int maxLength)
      : this(name, dbType, true, maxLength)
    {
    }

    public SqlMetaData(string name, VistaDBType dbType, bool allowNull, int maxLength)
    {
      this.Name = name;
      this.VistaDBType = dbType;
      this.AllowNull = allowNull;
      this.MaxLength = maxLength;
    }

    public VistaDBType VistaDBType { get; private set; }

    public string Name { get; private set; }

    public bool AllowNull { get; private set; }

    public int MaxLength { get; private set; }
  }
}
