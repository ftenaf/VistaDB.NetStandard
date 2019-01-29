using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class AlterAssemblyStatement : CreateAssemblyStatement
  {
    public AlterAssemblyStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override IQueryResult OnExecuteQuery()
    {
      this.Database.UpdateAssembly(this.name, this.fileName, this.description);
      return (IQueryResult) null;
    }
  }
}
