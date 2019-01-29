using VistaDB.Engine.Internal;
using VistaDB.Provider;

namespace VistaDB.Engine.SQL
{
  internal class SelectTableStatement : BaseSelectStatement
  {
    private IQueryResult table;
    private IQuerySchemaInfo schema;
    private bool tableSent;

    internal SelectTableStatement(LocalSQLConnection connection, SQLParser parser, long queryId, IQueryResult table, IQuerySchemaInfo schema)
      : base(connection, (Statement) null, parser, queryId)
    {
      this.table = table;
      this.schema = schema;
      this.tableSent = false;
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
    }

    public override void ResetResult()
    {
      this.table.FirstRow();
      this.tableSent = false;
    }

    public override INextQueryResult NextResult(VistaDBPipe pipe)
    {
      if (this.tableSent)
        return (INextQueryResult) null;
      this.tableSent = true;
      return (INextQueryResult) new BatchStatement.ResultSetData(this.table, this.schema, this.table.RowCount);
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
      return this.schema;
    }

    protected override VistaDBType OnPrepareQuery()
    {
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      return (IQueryResult) null;
    }
  }
}
