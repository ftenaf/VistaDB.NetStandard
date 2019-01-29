using System;
using System.Runtime.Serialization;

namespace VistaDB.Engine.Core
{
  [Serializable]
  public class VistaDBLicenseException : Exception
  {
    public VistaDBLicenseException()
    {
    }

    public VistaDBLicenseException(string message)
      : base(message)
    {
    }

    public VistaDBLicenseException(string message, Exception innerException)
      : base(message, innerException)
    {
    }

    protected VistaDBLicenseException(SerializationInfo info, StreamingContext context)
      : base(info, context)
    {
    }
  }
}
