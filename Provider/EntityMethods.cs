using System;
using System.Reflection;

namespace VistaDB.Provider
{
  internal static class EntityMethods
  {
    internal static Type SystemDataCommonDbProviderServicesType = Type.GetType("System.Data.Common.DbProviderServices, System.Data.Entity, Version=3.5.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089", false);
    private const string PublicKeyToken = "dfc935afe2125461";
    private const string AssemblyVersion = "4.1.0.0";
    private const string VistaDBProviderServicesFullName = "VistaDB.Provider.VistaDBProviderServices, VistaDB.Entities.4, Version=4.1.0.0, Culture=neutral, PublicKeyToken=dfc935afe2125461";
    private static FieldInfo VistaDBProviderServicesInstanceFieldInfo;

    internal static object VistaDBProviderServicesInstance()
    {
      if (EntityMethods.VistaDBProviderServicesInstanceFieldInfo == null)
      {
        Type type = Type.GetType("VistaDB.Provider.VistaDBProviderServices, VistaDB.Entities.4, Version=4.1.0.0, Culture=neutral, PublicKeyToken=dfc935afe2125461", false);
        if (type != null)
          EntityMethods.VistaDBProviderServicesInstanceFieldInfo = type.GetField("Instance");
      }
      if (EntityMethods.VistaDBProviderServicesInstanceFieldInfo != null)
        return EntityMethods.VistaDBProviderServicesInstanceFieldInfo.GetValue((object) null);
      return (object) null;
    }
  }
}
