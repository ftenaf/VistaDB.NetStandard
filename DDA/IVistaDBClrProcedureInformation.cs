
namespace VistaDB.DDA
{
  public interface IVistaDBClrProcedureInformation : IVistaDBDatabaseObject
  {
    new string Name { get; }

    string Signature { get; }

    string AssemblyName { get; }

    string FullHostedName { get; }
  }
}
