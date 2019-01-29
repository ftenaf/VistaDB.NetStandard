using System;
using System.ComponentModel;

namespace VistaDB.Engine.Core
{
  public class VistaDBEngineLicense : License
  {
    private readonly Type m_Type;
    private readonly string m_Key;

    public VistaDBEngineLicense(Type type, string key)
    {
      this.m_Type = type;
      this.m_Key = key;
    }

    public VistaDBEngineLicense(Type type)
    {
      this.m_Type = type;
      this.m_Key = string.Empty;
    }

    public override string LicenseKey
    {
      get
      {
        return this.m_Key;
      }
    }

    public Type LicensedType
    {
      get
      {
        return this.m_Type;
      }
    }

    public override void Dispose()
    {
    }
  }
}
