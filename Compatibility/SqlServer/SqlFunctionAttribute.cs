using System;

namespace VistaDB.Compatibility.SqlServer
{
  public class SqlFunctionAttribute : Attribute
  {
    public SqlFunctionAttribute()
    {
      DataAccess = DataAccessKind.None;
      FillRowMethodName = null;
      IsDeterministic = false;
      IsPrecise = false;
      Name = null;
      SystemDataAccess = SystemDataAccessKind.None;
      TableDefinition = null;
    }

    public DataAccessKind DataAccess { get; set; }

    public string FillRowMethodName { get; set; }

    public bool IsDeterministic { get; set; }

    public bool IsPrecise { get; set; }

    public string Name { get; set; }

    public SystemDataAccessKind SystemDataAccess { get; set; }

    public string TableDefinition { get; set; }
  }
}
