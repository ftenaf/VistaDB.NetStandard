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
      : base(connection, null, parser, queryId)
    {
      this.table = table;
      this.schema = schema;
      tableSent = false;
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
    }

    public override void ResetResult()
    {
      table.FirstRow();
      tableSent = false;
    }

    public override INextQueryResult NextResult(VistaDBPipe pipe)
    {
      if (tableSent)
        return null;
      tableSent = true;
      return new BatchStatement.ResultSetData(table, schema, table.RowCount);
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
      return schema;
    }

    protected override VistaDBType OnPrepareQuery()
    {
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      return null;
    }
  }
}
