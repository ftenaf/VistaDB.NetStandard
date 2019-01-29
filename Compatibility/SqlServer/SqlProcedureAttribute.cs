using System;

namespace VistaDB.Compatibility.SqlServer
{
  public sealed class SqlProcedureAttribute : Attribute
  {
    public SqlProcedureAttribute()
    {
      this.Name = (string) null;
    }

    public string Name { get; set; }
  }
}
