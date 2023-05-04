using System;

namespace VistaDB.Compatibility.SqlServer
{
  public sealed class SqlTriggerAttribute : Attribute
  {
    public SqlTriggerAttribute()
    {
      Event = null;
      Name = null;
      Target = null;
    }

    public string Event { get; set; }

    public string Name { get; set; }

    public string Target { get; set; }
  }
}
