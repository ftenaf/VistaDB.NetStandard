using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class DropViewStatement : DropTableStatement
  {
    public DropViewStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override IQueryResult OnExecuteQuery()
    {
      IViewList viewList = Database.EnumViews();
      foreach (string tableName in tableNames)
      {
        IView view = (IView) viewList[tableName];
        if (view == null)
          throw new VistaDBSQLException(606, tableName, lineNo, symbolNo);
        Database.DeleteViewObject(view);
      }
      return null;
    }
  }
}
