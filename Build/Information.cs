using System.Reflection;

namespace VistaDB.Build
{
  internal static class Information
  {
    internal const string Build = "34";
    internal const string Release = "3";
    internal const string Major = "4";
    internal const string Minor = "3";
    internal const string AssemblyMinor = "1";
    internal const string VDBExtension = "vdb4";
    internal const string DbCfgExtension = "vdc4";
    internal const string SqlExtension = "vsql4";
    internal const string LicExtension = "lic4";
    internal const string PerfExtension = "vdb4perf";
    internal const string LockExtension = "vdb4lck";
    internal const string InvariantString = "System.Data.VistaDB";
    internal const string Edition = "";
    internal const string Configuration = "Release";
    internal const string PLKAssembly = "4.1";
    internal const string PLKProduct = "VistaDB 4";
    internal const string PLKCompany = "Infinite Codex, Inc.";
    internal const string RegBase = "VistaDB Software Inc\\VistaDB4";
    internal const string VersionString = "4.1";
    internal const string BuildString = "4.3.3.34";
    internal const string EditionString = "";
    private static string _AssemblyFileVersionString;

    internal static string GetThisAssemblyFileVersion()
    {
      if (_AssemblyFileVersionString == null)
      {
        Assembly assembly = null;
        try
        {
          assembly = Assembly.GetExecutingAssembly();
          AssemblyFileVersionAttribute[] customAttributes = assembly.GetCustomAttributes(typeof (AssemblyFileVersionAttribute), true) as AssemblyFileVersionAttribute[];
                    _AssemblyFileVersionString = customAttributes == null || customAttributes.Length <= 0 ? assembly.GetName().Version.ToString() : customAttributes[0].Version ?? string.Empty;
        }
        catch
        {
                    _AssemblyFileVersionString = assembly == null ? string.Empty : assembly.GetName().Version.ToString();
        }
      }
      return _AssemblyFileVersionString;
    }
  }
}
