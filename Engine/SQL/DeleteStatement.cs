using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class DeleteStatement : BaseUpdateStatement
  {
    public DeleteStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      SQLParser.TokenValueClass tokenValue = parser.TokenValue;
      parser.SkipToken(true);
      if (parser.IsToken("FROM"))
        parser.SkipToken(true);
      if (tokenValue.TokenType != TokenType.Unknown && tokenValue.TokenType != TokenType.Name && tokenValue.TokenType != TokenType.ComplexName)
        throw new VistaDBSQLException(585, tokenValue.Token, this.lineNo, this.symbolNo);
      string tableName = parser.GetTableName((Statement) this);
      this.destinationTable = (SourceTable) new NativeSourceTable((Statement) this, tableName, tableName, 0, tokenValue.RowNo, tokenValue.ColNo);
      if (!parser.SkipToken(false))
        return;
      base.OnParse(connection, parser);
    }

    protected override bool AcceptRow()
    {
      if (!this.destinationTable.RowAvailable)
        return true;
      this.destinationTable.DeleteRow();
      ++this.affectedRows;
      return true;
    }

    protected override void ExecuteSimple()
    {
      this.destinationTable.DoOpenExternalRelationships(false, true);
      bool isAlwaysTrue = this.whereClause.IsAlwaysTrue;
      if (isAlwaysTrue)
        this.destinationTable.DoFreezeSelfRelationships();
      try
      {
        try
        {
          base.ExecuteSimple();
        }
        finally
        {
          this.destinationTable.DoFreeExternalRelationships();
        }
      }
      finally
      {
        if (isAlwaysTrue)
          this.destinationTable.DoDefreezeSelfRelationships();
      }
    }

    protected override void DoPrepareTriggers()
    {
      this.destinationTable.PrepareTriggers(TriggerAction.AfterDelete);
    }

    protected override void DoExecuteTriggers(bool justReset)
    {
      this.destinationTable.ExecuteTriggers(TriggerAction.AfterDelete, justReset);
    }

    public override void SetChanged()
    {
      this.whereClause.SetChanged();
    }
  }
}
