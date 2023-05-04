





using System;
using System.ComponentModel;
using System.Data.Common;
using System.Runtime.InteropServices;
using System.Security;
using System.Security.Permissions;

namespace VistaDB.Provider
{
  [Guid("3992B491-AAF4-4120-9B38-ECE53A592A52")]
  [Description("VistaDB 4 File")]
  public sealed class VistaDBProviderFactory : DbProviderFactory, IServiceProvider
  {
    public static readonly VistaDBProviderFactory Instance = new VistaDBProviderFactory();

    private VistaDBProviderFactory()
    {
    }

    public override bool CanCreateDataSourceEnumerator
    {
      get
      {
        return false;
      }
    }

    public override DbCommand CreateCommand()
    {
      return new VistaDBCommand();
    }

    public override DbCommandBuilder CreateCommandBuilder()
    {
      return new VistaDBCommandBuilder();
    }

    public override DbConnection CreateConnection()
    {
      return new VistaDBConnection();
    }

    public override DbConnectionStringBuilder CreateConnectionStringBuilder()
    {
      return new VistaDBConnectionStringBuilder();
    }

    public override DbDataAdapter CreateDataAdapter()
    {
      return new VistaDBDataAdapter();
    }

    public override DbParameter CreateParameter()
    {
      return new VistaDBParameter();
    }

    public CodeAccessPermission CreatePermission(PermissionState state)
    {
      return new VistaDBDataPermission();
    }

    object IServiceProvider.GetService(Type serviceType)
    {
      if (serviceType == EntityMethods.SystemDataCommonDbProviderServicesType)
        return EntityMethods.VistaDBProviderServicesInstance();
      return null;
    }
  }
}
