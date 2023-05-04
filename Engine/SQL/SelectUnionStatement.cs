using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class SelectUnionStatement : SelectStatement
  {
    internal SelectUnionStatement(LocalSQLConnection connection, Statement parent, SQLParser parser)
      : base(connection, parent, parser, 0L)
    {
      addRowMethod = new AddRowMethod(((SelectStatement) parent).AddRow);
    }

    protected override void DoBeforeParse()
    {
      base.DoBeforeParse();
    }
  }
}
