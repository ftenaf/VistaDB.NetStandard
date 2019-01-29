namespace VistaDB.DDA
{
  public interface IVistaDBConstraintInformation : IVistaDBDatabaseObject
  {
    new string Name { get; }

    string Expression { get; }

    bool AffectsInsertion { get; }

    bool AffectsUpdate { get; }

    bool AffectsDelete { get; }
  }
}
