using System;
using System.ComponentModel;

namespace VistaDB.Engine.Core
{
  public class VistaDBLicenseProvider : LicenseProvider
  {
    public override License GetLicense(LicenseContext context, Type type, object instance, bool allowExceptions)
    {
      if (context.UsageMode != LicenseUsageMode.Designtime)
        return new VistaDBEngineLicense(type);
      if (!allowExceptions)
        return null;
      throw new LicenseException(type, instance, "No valid activated license for VistaDB found.  Use the VistaDB DataBuilder application to activate a valid license.");
    }
  }
}
