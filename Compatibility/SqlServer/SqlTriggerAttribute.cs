using System;

namespace VistaDB.Compatibility.SqlServer
{
  public sealed class SqlTriggerAttribute : Attribute
  {
    public SqlTriggerAttribute()
    {
      Event = (string) null;
      Name = (string) null;
      Target = (string) null;
    }

    public string Event { get; set; }

    public string Name { get; set; }

    public string Target { get; set; }
  }
}
