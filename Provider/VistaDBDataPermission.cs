using System.Data.Common;
using System.Security.Permissions;

namespace VistaDB.Provider
{
  public sealed class VistaDBDataPermission : DBDataPermission
  {
    public VistaDBDataPermission()
      : base(PermissionState.None)
    {
    }
  }
}
