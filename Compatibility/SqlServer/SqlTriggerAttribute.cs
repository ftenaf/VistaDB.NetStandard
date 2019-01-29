using System;

namespace VistaDB.Compatibility.SqlServer
{
  public sealed class SqlTriggerAttribute : Attribute
  {
    public SqlTriggerAttribute()
    {
      this.Event = (string) null;
      this.Name = (string) null;
      this.Target = (string) null;
    }

    public string Event { get; set; }

    public string Name { get; set; }

    public string Target { get; set; }
  }
}
