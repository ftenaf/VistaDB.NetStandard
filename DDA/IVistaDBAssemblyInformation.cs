namespace VistaDB.DDA
{
  public interface IVistaDBAssemblyInformation : IVistaDBDatabaseObject
  {
    new string Name { get; }

    string FullName { get; }

    string ImageRuntimeVersion { get; }

    string VistaDBRuntimeVersion { get; }

    IVistaDBClrProcedureCollection Procedures { get; }

    IVistaDBClrTriggerCollection Triggers { get; }

    byte[] COFFImage { get; }
  }
}
