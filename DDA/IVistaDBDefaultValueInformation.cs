namespace VistaDB.DDA
{
  public interface IVistaDBDefaultValueInformation : IVistaDBDatabaseObject
  {
    string ColumnName { get; }

    string Expression { get; }

    bool UseInUpdate { get; }
  }
}
