using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class SetGroupSynchronizationStatement : Statement
  {
    private bool synchronization;

    public SetGroupSynchronizationStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      parser.ExpectedExpression("SYNCHRONIZATION");
      parser.SkipToken(true);
      if (parser.IsToken("ON"))
      {
        synchronization = true;
      }
      else
      {
        parser.ExpectedExpression("OFF");
        synchronization = false;
      }
      parser.SkipToken(false);
    }

    protected override VistaDBType OnPrepareQuery()
    {
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      connection.SetGroupSynchronization(synchronization);
      return null;
    }
  }
}
