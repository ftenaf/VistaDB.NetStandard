using System;
using System.Reflection;

namespace VistaDB.Provider
{
  internal static class EntityMethods
  {
    internal static Type SystemDataCommonDbProviderServicesType = Type.GetType("System.Data.Common.DbProviderServices, System.Data.Entity, Version=3.5.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089", false);
        private static FieldInfo VistaDBProviderServicesInstanceFieldInfo;

    internal static object VistaDBProviderServicesInstance()
    {
      if (VistaDBProviderServicesInstanceFieldInfo == null)
      {
        Type type = Type.GetType("VistaDB.Provider.VistaDBProviderServices, VistaDB.Entities.4, Version=4.1.0.0, Culture=neutral, PublicKeyToken=dfc935afe2125461", false);
        if (type != null)
                    VistaDBProviderServicesInstanceFieldInfo = type.GetField("Instance");
      }
      if (VistaDBProviderServicesInstanceFieldInfo != null)
        return VistaDBProviderServicesInstanceFieldInfo.GetValue(null);
      return null;
    }
  }
}
