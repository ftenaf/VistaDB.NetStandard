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
      Database.UpdateAssembly(name, fileName, description);
      return null;
    }
  }
}
