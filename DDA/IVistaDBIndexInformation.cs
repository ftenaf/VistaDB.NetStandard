namespace VistaDB.DDA
{
  public interface IVistaDBIndexInformation : IVistaDBDatabaseObject
  {
    new string Name { get; }

    string KeyExpression { get; }

    IVistaDBKeyColumn[] KeyStructure { get; }

    bool Primary { get; }

    bool Unique { get; }

    bool FKConstraint { get; }

    bool FullTextSearch { get; }

    bool Temporary { get; }
  }
}
