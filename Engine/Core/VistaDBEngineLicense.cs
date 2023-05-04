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
      m_Type = type;
      m_Key = key;
    }

    public VistaDBEngineLicense(Type type)
    {
      m_Type = type;
      m_Key = string.Empty;
    }

    public override string LicenseKey
    {
      get
      {
        return m_Key;
      }
    }

    public Type LicensedType
    {
      get
      {
        return m_Type;
      }
    }

    public override void Dispose()
    {
    }
  }
}
