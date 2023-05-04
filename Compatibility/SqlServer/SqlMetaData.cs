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
      Name = name;
      VistaDBType = dbType;
      AllowNull = allowNull;
    }

    public SqlMetaData(string name, VistaDBType dbType, int maxLength)
      : this(name, dbType, true, maxLength)
    {
    }

    public SqlMetaData(string name, VistaDBType dbType, bool allowNull, int maxLength)
    {
      Name = name;
      VistaDBType = dbType;
      AllowNull = allowNull;
      MaxLength = maxLength;
    }

    public VistaDBType VistaDBType { get; private set; }

    public string Name { get; private set; }

    public bool AllowNull { get; private set; }

    public int MaxLength { get; private set; }
  }
}
