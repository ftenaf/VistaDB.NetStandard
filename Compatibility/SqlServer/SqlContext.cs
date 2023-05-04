using System.Security.Principal;
using VistaDB.Provider;

namespace VistaDB.Compatibility.SqlServer
{
  public sealed class SqlContext
  {
    static SqlContext()
    {
            WindowsIdentity = WindowsIdentity.GetCurrent();
    }

    public static bool IsAvailable
    {
      get
      {
        return VistaDBContext.SQLChannel.IsAvailable;
      }
    }

    public static VistaDBPipe Pipe
    {
      get
      {
        return VistaDBContext.SQLChannel.Pipe;
      }
    }

    public static TriggerContext TriggerContext
    {
      get
      {
        return VistaDBContext.SQLChannel.TriggerContext;
      }
    }

    public static WindowsIdentity WindowsIdentity { get; private set; }
  }
}
