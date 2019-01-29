using System;

namespace VistaDB.Compatibility.SqlServer
{
  public class SqlFunctionAttribute : Attribute
  {
    public SqlFunctionAttribute()
    {
      this.DataAccess = DataAccessKind.None;
      this.FillRowMethodName = (string) null;
      this.IsDeterministic = false;
      this.IsPrecise = false;
      this.Name = (string) null;
      this.SystemDataAccess = SystemDataAccessKind.None;
      this.TableDefinition = (string) null;
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
