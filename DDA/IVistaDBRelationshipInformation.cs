namespace VistaDB.DDA
{
  public interface IVistaDBRelationshipInformation : IVistaDBDatabaseObject
  {
    new string Name { get; }

    string PrimaryTable { get; }

    string ForeignTable { get; }

    string ForeignKey { get; }

    VistaDBReferentialIntegrity DeleteIntegrity { get; }

    VistaDBReferentialIntegrity UpdateIntegrity { get; }
  }
}
