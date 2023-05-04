using System;

namespace VistaDB.Compatibility.SqlServer
{
  public sealed class SqlProcedureAttribute : Attribute
  {
    public SqlProcedureAttribute()
    {
      Name = null;
    }

    public string Name { get; set; }
  }
}
