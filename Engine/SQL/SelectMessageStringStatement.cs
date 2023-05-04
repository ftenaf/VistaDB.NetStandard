using VistaDB.Engine.Internal;
using VistaDB.Provider;

namespace VistaDB.Engine.SQL
{
  internal class SelectMessageStringStatement : BaseSelectStatement
  {
    private MessageString messageResult;
    private bool messageSent;

    internal SelectMessageStringStatement(LocalSQLConnection connection, SQLParser parser, long queryId, string message)
      : base(connection, null, parser, queryId)
    {
      messageResult = new MessageString(null, message);
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
    }

    public override void ResetResult()
    {
      messageSent = false;
    }

    public override INextQueryResult NextResult(VistaDBPipe pipe)
    {
      if (messageSent)
        return null;
      messageSent = true;
      return new BatchStatement.ResultSetData(messageResult, GetSchemaInfo(), 1L);
    }

    protected override bool AcceptRow()
    {
      return true;
    }

    public override void SetChanged()
    {
    }

    public override IQuerySchemaInfo GetSchemaInfo()
    {
      return messageResult;
    }

    protected override VistaDBType OnPrepareQuery()
    {
      return VistaDBType.NVarChar;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      return null;
    }
  }
}
