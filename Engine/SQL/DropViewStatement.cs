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
      IViewList viewList = this.Database.EnumViews();
      foreach (string tableName in this.tableNames)
      {
        IView view = (IView) viewList[(object) tableName];
        if (view == null)
          throw new VistaDBSQLException(606, tableName, this.lineNo, this.symbolNo);
        this.Database.DeleteViewObject(view);
      }
      return (IQueryResult) null;
    }
  }
}
