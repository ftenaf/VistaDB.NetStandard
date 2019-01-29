namespace VistaDB.DDA
{
  public interface IVistaDBIdentityInformation : IVistaDBDatabaseObject
  {
    string ColumnName { get; }

    string StepExpression { get; }

    string SeedValue { get; }
  }
}
