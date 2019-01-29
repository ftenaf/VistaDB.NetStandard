namespace VistaDB.DDA
{
  public interface IVistaDBClrTriggerInformation : IVistaDBDatabaseObject
  {
    new string Name { get; }

    string Signature { get; }

    string AssemblyName { get; }

    string FullHostedName { get; }

    string TableName { get; }

    TriggerAction TriggerAction { get; }
  }
}
