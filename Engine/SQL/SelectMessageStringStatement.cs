using VistaDB.Engine.Internal;
using VistaDB.Provider;

namespace VistaDB.Engine.SQL
{
  internal class SelectMessageStringStatement : BaseSelectStatement
  {
    private MessageString messageResult;
    private bool messageSent;

    internal SelectMessageStringStatement(LocalSQLConnection connection, SQLParser parser, long queryId, string message)
      : base(connection, (Statement) null, parser, queryId)
    {
      this.messageResult = new MessageString((IDatabase) null, message);
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
    }

    public override void ResetResult()
    {
      this.messageSent = false;
    }

    public override INextQueryResult NextResult(VistaDBPipe pipe)
    {
      if (this.messageSent)
        return (INextQueryResult) null;
      this.messageSent = true;
      return (INextQueryResult) new BatchStatement.ResultSetData((IQueryResult) this.messageResult, this.GetSchemaInfo(), 1L);
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
      return (IQuerySchemaInfo) this.messageResult;
    }

    protected override VistaDBType OnPrepareQuery()
    {
      return VistaDBType.NVarChar;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      return (IQueryResult) null;
    }
  }
}
