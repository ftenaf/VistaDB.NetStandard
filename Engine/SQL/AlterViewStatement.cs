using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class AlterViewStatement : CreateViewStatement
  {
    public AlterViewStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
      this.replaceAlter = true;
    }

    protected override void CheckView(IViewList views, string name)
    {
      if (!views.Contains((object) name))
        throw new VistaDBSQLException(606, name, this.lineNo, this.symbolNo);
    }
  }
}
