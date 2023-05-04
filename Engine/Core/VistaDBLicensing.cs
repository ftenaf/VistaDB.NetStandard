





using System;
using System.Collections;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Security.Permissions;
using System.Security.Principal;
using System.Text;

namespace VistaDB.Engine.Core
{
  internal static class VistaDBLicensing
  {
    internal static int VistaDBProductCode = 2;
    private static DateTimeOffset g_LicensingNextCheck = DateTimeOffset.MinValue;
    private static string g_LicensingBasePath = Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData);
    internal const string MachineKeyContainerName = "Gibraltar_Activation_Enc_Key";
    internal const string BadTrialKey = "BAD";
        internal const string TempFileExtension = ".new";
    internal const string VistaDBLibraryPublicKey = "0024000004800000940000000602000000240000525341310004000001000100252242ea001093a5f468db7756d556ace963806e5564832f9346305d47a4dd81a896a485617cf8006e37e4a25048df0bdcac004cf5f339e22aa77aa243a0f67f41732f85ef47d151768807623973f2980a0f91ea8926c80bbc1d5fa662ad795ee6b62293c503fef640eee09b1c13bc0d7068d03904e69b86f5f653a8fa1fe2b1";
    internal const string VistaDBLibraryPublicKeyToken = "38k1r+ISVGE=";
    internal const string GibraltarLibraryPublicKey = "00240000048000009400000006020000002400005253413100040000010001000fb2ab13e9db180c89e558e0ac32d517f34ddd626fa40293275378577e4a202d2c8095b2327eaac86dc884333d41b1763cfaad61c7bc7e9e959739f08854d71024feff627e8ef86945f430062c4d959bc50da3d27198db758498f406899ab06f1e32fcb6b213525d751e97ec0aa06776bfd21cc9992775a627c317e231d6adc7";
    internal const string GibraltarLibraryPublicKeyToken = "ykKh7o0uQtM=";
        private static volatile LicenseActivationStatus g_LicensingStatus;

    static VistaDBLicensing()
    {
            g_LicensingBasePath = Path.Combine(g_LicensingBasePath, "Gibraltar");
            g_LicensingBasePath = Path.Combine(g_LicensingBasePath, "Licensing");
    }

    internal static bool HasLicensingFolder
    {
      get
      {
        try
        {
          return Directory.Exists(g_LicensingBasePath);
        }
        catch
        {
          return false;
        }
      }
    }

    [Conditional("TRIAL")]
    public static void CheckEngineLicensing()
    {
      if (g_LicensingStatus >= LicenseActivationStatus.MinimumAcceptedLicense || !Debugger.IsAttached)
        return;
      if (g_LicensingNextCheck <= DateTimeOffset.Now)
      {
        if (CheckLicenseActivation(1, 1))
                    g_LicensingStatus = LicenseActivationStatus.MinimumAcceptedLicense;
                g_LicensingNextCheck = DateTimeOffset.Now.AddSeconds(10.0);
      }
      if (g_LicensingStatus < LicenseActivationStatus.MinimumAcceptedLicense)
        throw new VistaDBLicenseException("The evaluation version of the VistaDB engine requires an activated license on the computer it runs on.  Use the DataBuilder app or the plug-in for Visual Studio to activate your trial key (or full license key).  If you have purchased a full license make sure to reinstall VistaDB and provide your license key in the install wizard to install the licensed version of the engine and then recompile your applications which reference it.");
    }

    internal static bool CheckLicenseActivation(int applicationCode, int featureCode)
    {
      if (!HasLicensingFolder)
        return false;
      string path2 = string.Format("Feature_{0}_{1}_{2:000}.glk", VistaDBProductCode, applicationCode, featureCode);
      string path = Path.Combine(g_LicensingBasePath, path2);
      if (!File.Exists(path))
        return false;
      ActivationKey activationKey;
      try
      {
        activationKey = new ActivationKey(Convert.FromBase64String(File.ReadAllText(path, Encoding.UTF8)));
      }
      catch
      {
        return false;
      }
      string licenseKey = activationKey.LicenseKey;
      if (!activationKey.IsValid || !ApplicationFeatureKey.IsValidKey(licenseKey))
        return false;
      ApplicationFeatureKey applicationFeatureKey = new ApplicationFeatureKey(licenseKey);
      return applicationFeatureKey.HasValidChecksum && applicationFeatureKey.ProductCode == VistaDBProductCode && (applicationFeatureKey.ApplicationCode == applicationCode && applicationFeatureKey.FeatureCode == featureCode) && (!activationKey.ExpirationDt.HasValue || !(activationKey.ExpirationDt.Value < DateTimeOffset.Now));
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    [StrongNameIdentityPermission(SecurityAction.LinkDemand, PublicKey = "0024000004800000940000000602000000240000525341310004000001000100252242ea001093a5f468db7756d556ace963806e5564832f9346305d47a4dd81a896a485617cf8006e37e4a25048df0bdcac004cf5f339e22aa77aa243a0f67f41732f85ef47d151768807623973f2980a0f91ea8926c80bbc1d5fa662ad795ee6b62293c503fef640eee09b1c13bc0d7068d03904e69b86f5f653a8fa1fe2b1")]
    internal static void FixOwnerPermissions(CspKeyContainerInfo containerInfo, bool poison, bool persisting)
    {
            FixKeyPermissions(containerInfo.UniqueKeyContainerName, poison, null, persisting);
    }

    [StrongNameIdentityPermission(SecurityAction.LinkDemand, PublicKey = "0024000004800000940000000602000000240000525341310004000001000100252242ea001093a5f468db7756d556ace963806e5564832f9346305d47a4dd81a896a485617cf8006e37e4a25048df0bdcac004cf5f339e22aa77aa243a0f67f41732f85ef47d151768807623973f2980a0f91ea8926c80bbc1d5fa662ad795ee6b62293c503fef640eee09b1c13bc0d7068d03904e69b86f5f653a8fa1fe2b1")]
    internal static void FixKeyPermissions(string uniqueKeyName, bool poison, string label, bool persisting)
    {
            ValidateCallStack(2);
      if (string.IsNullOrEmpty(uniqueKeyName))
        return;
      try
      {
        string str1 = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData), "Microsoft");
        if (!Directory.Exists(str1))
          return;
        string str2 = Path.Combine(str1, "Crypto");
        if (!Directory.Exists(str2))
          return;
        string str3 = Path.Combine(str2, poison ? "DSS" : "RSA");
        if (!Directory.Exists(str3))
          return;
        string path1 = Path.Combine(str3, "MachineKeys");
        if (!Directory.Exists(path1))
          return;
        string[] files = Directory.GetFiles(path1, uniqueKeyName);
        if (files.Length != 1)
          return;
        SecurityIdentifier securityIdentifier1 = new SecurityIdentifier(WellKnownSidType.BuiltinUsersSid, null);
        SecurityIdentifier securityIdentifier2 = new SecurityIdentifier(WellKnownSidType.BuiltinAdministratorsSid, null);
        SecurityIdentifier securityIdentifier3 = new SecurityIdentifier(WellKnownSidType.CreatorOwnerSid, null);
        FileSystemAccessRule rule1 = new FileSystemAccessRule(securityIdentifier1, FileSystemRights.Modify, AccessControlType.Allow);
        FileSystemAccessRule rule2 = new FileSystemAccessRule(securityIdentifier2, FileSystemRights.Modify, AccessControlType.Allow);
        FileSystemAccessRule rule3 = new FileSystemAccessRule(securityIdentifier3, FileSystemRights.FullControl, AccessControlType.Allow);
        foreach (string path2 in files)
        {
          try
          {
            if (!persisting)
            {
			  FileSecurity accessControl = new FileSecurity(path2, AccessControlSections.All);
              accessControl.GetAccessRules(true, false, typeof (SecurityIdentifier));
              accessControl.AddAccessRule(rule3);
			  //File.SetAccessControl(path2, accessControl);
            }
            else if (File.Exists(path2))
            {
              bool flag1 = true;
              bool flag2 = true;
              FileSecurity accessControl = new FileSecurity(path2, AccessControlSections.All); //File.GetAccessControl(path2);
              foreach (AuthorizationRule accessRule in (ReadOnlyCollectionBase) accessControl.GetAccessRules(true, false, typeof (SecurityIdentifier)))
              {
                FileSystemAccessRule systemAccessRule = accessRule as FileSystemAccessRule;
                if (systemAccessRule != null && systemAccessRule.AccessControlType == AccessControlType.Allow && (systemAccessRule.FileSystemRights & FileSystemRights.Modify) == FileSystemRights.Modify)
                {
                  if (systemAccessRule.IdentityReference == securityIdentifier1)
                    flag1 = false;
                  else if (systemAccessRule.IdentityReference == securityIdentifier2)
                    flag2 = false;
                }
              }
              if (!flag1)
              {
                if (!flag2)
                  continue;
              }
              accessControl.AddAccessRule(rule3);
              accessControl.AddAccessRule(rule1);
              accessControl.AddAccessRule(rule2);
              //File.SetAccessControl(path2, accessControl);
            }
          }
          catch
          {
          }
        }
      }
      catch
      {
      }
    }

    internal static void ValidateCallStack(int frameCount)
    {
      if (frameCount < 0)
        frameCount = 0;
      ++frameCount;
      try
      {
        string base64String1 = Convert.ToBase64String(Assembly.GetExecutingAssembly().GetName().GetPublicKeyToken());
        if (base64String1 != "38k1r+ISVGE=" && base64String1 != "ykKh7o0uQtM=")
          throw new VistaDBLicenseException("Invalid call stack: Public key not recognized: " + base64String1);
        for (int skipFrames = 1; skipFrames <= frameCount; ++skipFrames)
        {
          MethodBase method = new StackFrame(skipFrames, false).GetMethod();
          if (method == null)
            throw new VistaDBLicenseException("Invalid call stack: Stack was shorter than requested validation.");
          AssemblyName name = method.Module.Assembly.GetName();
          string base64String2 = Convert.ToBase64String(name.GetPublicKeyToken());
          if (base64String2 != "38k1r+ISVGE=" && base64String2 != "ykKh7o0uQtM=")
            throw new VistaDBLicenseException("Invalid call stack: Public key did not match: " + ToBase16Transform(name.GetPublicKey()));
          if (skipFrames >= frameCount)
          {
            foreach (StrongNameIdentityPermissionAttribute permissionAttribute in Attribute.GetCustomAttributes(method, typeof (StrongNameIdentityPermissionAttribute)) ?? new Attribute[0])
            {
              if (permissionAttribute != null && permissionAttribute.Action == SecurityAction.LinkDemand && (permissionAttribute.PublicKey == "0024000004800000940000000602000000240000525341310004000001000100252242ea001093a5f468db7756d556ace963806e5564832f9346305d47a4dd81a896a485617cf8006e37e4a25048df0bdcac004cf5f339e22aa77aa243a0f67f41732f85ef47d151768807623973f2980a0f91ea8926c80bbc1d5fa662ad795ee6b62293c503fef640eee09b1c13bc0d7068d03904e69b86f5f653a8fa1fe2b1" || permissionAttribute.PublicKey == "00240000048000009400000006020000002400005253413100040000010001000fb2ab13e9db180c89e558e0ac32d517f34ddd626fa40293275378577e4a202d2c8095b2327eaac86dc884333d41b1763cfaad61c7bc7e9e959739f08854d71024feff627e8ef86945f430062c4d959bc50da3d27198db758498f406899ab06f1e32fcb6b213525d751e97ec0aa06776bfd21cc9992775a627c317e231d6adc7"))
              {
                frameCount = skipFrames + 1;
                break;
              }
            }
          }
        }
      }
      catch (VistaDBLicenseException ex)
      {
        GC.KeepAlive(ex);
        throw;
      }
      catch (Exception ex)
      {
        throw new VistaDBLicenseException("Unexpected error validating call stack.", ex);
      }
    }

    private static string ToBase16Transform(byte[] value)
    {
      return BitConverter.ToString(value).Replace("-", string.Empty).ToLowerInvariant();
    }

    [StrongNameIdentityPermission(SecurityAction.LinkDemand, PublicKey = "0024000004800000940000000602000000240000525341310004000001000100252242ea001093a5f468db7756d556ace963806e5564832f9346305d47a4dd81a896a485617cf8006e37e4a25048df0bdcac004cf5f339e22aa77aa243a0f67f41732f85ef47d151768807623973f2980a0f91ea8926c80bbc1d5fa662ad795ee6b62293c503fef640eee09b1c13bc0d7068d03904e69b86f5f653a8fa1fe2b1")]
    internal static RSACryptoServiceProvider GetLocalKey()
    {
            ValidateCallStack(2);
      RSACryptoServiceProvider cryptoServiceProvider = null;
      CspParameters parameters = new CspParameters();
      parameters.KeyContainerName = "Gibraltar_Activation_Enc_Key";
      parameters.Flags = CspProviderFlags.UseMachineKeyStore | CspProviderFlags.UseExistingKey;
      try
      {
        cryptoServiceProvider = new RSACryptoServiceProvider(parameters);
        CspKeyContainerInfo keyContainerInfo = cryptoServiceProvider.CspKeyContainerInfo;
        int keySize = cryptoServiceProvider.KeySize;
        bool publicOnly = cryptoServiceProvider.PublicOnly;
        bool flag1 = false;
        if (keyContainerInfo.Accessible)
        {
          bool exportable = keyContainerInfo.Exportable;
          bool hardwareDevice = keyContainerInfo.HardwareDevice;
          bool flag2 = keyContainerInfo.Protected;
          if (publicOnly || exportable || (flag2 || hardwareDevice))
            flag1 = true;
          if (keySize != 1024 && keySize != 2048 && keySize != 1536)
            flag1 = true;
        }
        else
          flag1 = true;
        if (flag1)
        {
          //cryptoServiceProvider.Dispose();
          cryptoServiceProvider = null;
        }
      }
      catch (Exception)
            {
        if (cryptoServiceProvider != null)
        {
          //cryptoServiceProvider.Dispose();
          cryptoServiceProvider = null;
        }
      }
      return cryptoServiceProvider;
    }
  }
}
