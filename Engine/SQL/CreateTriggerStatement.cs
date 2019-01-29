using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class CreateTriggerStatement : BaseCreateStatement
  {
    private string triggerName;
    private string tableName;
    private string methodName;
    private string assemblyName;
    private string description;
    private bool onInsert;
    private bool onUpdate;
    private bool onDelete;
    private bool insteadOf;

    internal CreateTriggerStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      parser.SkipToken(true);
      this.triggerName = parser.TokenValue.Token;
      parser.SkipToken(true);
      if (parser.IsToken("DESCRIPTION"))
      {
        parser.SkipToken(true);
        this.description = parser.TokenValue.Token;
        parser.SkipToken(true);
      }
      else
        this.description = (string) null;
      parser.ExpectedExpression("ON");
      parser.SkipToken(true);
      this.tableName = parser.GetTableName((Statement) this);
      parser.SkipToken(true);
      string token1 = parser.TokenValue.Token;
      if (token1 == null)
        throw new VistaDBSQLException(631, token1, parser.TokenValue.RowNo, parser.TokenValue.ColNo);
      switch (token1.ToUpperInvariant())
      {
        case "FOR":
        case "AFTER":
          this.insteadOf = false;
          break;
        case "INSTEAD":
          parser.ExpectedExpression("OF");
          this.insteadOf = true;
          break;
        default:
          throw new VistaDBSQLException(631, token1, parser.TokenValue.RowNo, parser.TokenValue.ColNo);
      }
      parser.SkipToken(true);
      SQLParser.TokenValueClass tokenValue = parser.TokenValue;
      while (tokenValue.Token != null)
      {
        switch (tokenValue.Token.ToUpperInvariant())
        {
          case "INSERT":
            if (this.onInsert)
              throw new VistaDBSQLException(630, "INSERT", tokenValue.RowNo, tokenValue.ColNo);
            this.onInsert = true;
            break;
          case "UPDATE":
            if (this.onUpdate)
              throw new VistaDBSQLException(630, "UPDATE", tokenValue.RowNo, tokenValue.ColNo);
            this.onUpdate = true;
            break;
          case "DELETE":
            if (this.onDelete)
              throw new VistaDBSQLException(630, "DELETE", tokenValue.RowNo, tokenValue.ColNo);
            this.onDelete = true;
            break;
          default:
            throw new VistaDBSQLException(632, string.Format("{0}{1}{0}", (object) '\'', (object) tokenValue.Token), tokenValue.RowNo, tokenValue.ColNo);
        }
        parser.SkipToken(true);
        if (parser.IsToken(","))
        {
          parser.SkipToken(true);
        }
        else
        {
          parser.ExpectedExpression("AS");
          parser.SkipToken(true);
          parser.ExpectedExpression("EXTERNAL");
          parser.SkipToken(true);
          parser.ExpectedExpression("NAME");
          parser.SkipToken(true);
          string token2 = parser.TokenValue.Token;
          int length = token2.IndexOf(".");
          if (length <= 0)
            throw new VistaDBSQLException(613, this.methodName, parser.TokenValue.RowNo, parser.TokenValue.ColNo);
          this.assemblyName = token2.Substring(0, length);
          this.methodName = token2.Substring(length + 1, token2.Length - length - 1);
          parser.SkipToken(false);
          return;
        }
      }
      throw new VistaDBSQLException(632, string.Format("{0}{1}{0}", (object) '\'', (object) tokenValue.Token), tokenValue.RowNo, tokenValue.ColNo);
    }

    protected override IQueryResult OnExecuteQuery()
    {
      base.OnExecuteQuery();
      TriggerAction triggerAction = (TriggerAction) 0;
      if (!this.insteadOf)
      {
        if (this.onInsert)
          triggerAction |= TriggerAction.AfterInsert;
        if (this.onUpdate)
          triggerAction |= TriggerAction.AfterUpdate;
        if (this.onDelete)
          triggerAction |= TriggerAction.AfterDelete;
      }
      this.Database.RegisterClrTrigger(this.triggerName, this.methodName, this.assemblyName, this.tableName, triggerAction, this.description);
      return (IQueryResult) null;
    }
  }
}
