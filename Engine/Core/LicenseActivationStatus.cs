namespace VistaDB.Engine.Core
{
  internal enum LicenseActivationStatus
  {
    Unknown,
    NotFound,
    NoActivation,
    BadActivationFile,
    KeyNotActivated,
    ExpiredTrialActivation,
    ExpiredLicenseActivation,
    OldTrialVersion,
    OldLicenseVersion,
    OldPermanentVersion,
    MinimumAcceptedLicense,
    OldTrialMaintenanceOk,
    OldLicenseMaintenanceOk,
    OldPermanentMaintenanceOk,
    ValidTrialActivation,
    ValidLicenseActivation,
    ValidPermanentLicense,
  }
}
