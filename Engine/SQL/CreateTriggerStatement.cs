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
      triggerName = parser.TokenValue.Token;
      parser.SkipToken(true);
      if (parser.IsToken("DESCRIPTION"))
      {
        parser.SkipToken(true);
        description = parser.TokenValue.Token;
        parser.SkipToken(true);
      }
      else
        description = null;
      parser.ExpectedExpression("ON");
      parser.SkipToken(true);
      tableName = parser.GetTableName(this);
      parser.SkipToken(true);
      string token1 = parser.TokenValue.Token;
      if (token1 == null)
        throw new VistaDBSQLException(631, token1, parser.TokenValue.RowNo, parser.TokenValue.ColNo);
      switch (token1.ToUpperInvariant())
      {
        case "FOR":
        case "AFTER":
          insteadOf = false;
          break;
        case "INSTEAD":
          parser.ExpectedExpression("OF");
          insteadOf = true;
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
            if (onInsert)
              throw new VistaDBSQLException(630, "INSERT", tokenValue.RowNo, tokenValue.ColNo);
            onInsert = true;
            break;
          case "UPDATE":
            if (onUpdate)
              throw new VistaDBSQLException(630, "UPDATE", tokenValue.RowNo, tokenValue.ColNo);
            onUpdate = true;
            break;
          case "DELETE":
            if (onDelete)
              throw new VistaDBSQLException(630, "DELETE", tokenValue.RowNo, tokenValue.ColNo);
            onDelete = true;
            break;
          default:
            throw new VistaDBSQLException(632, string.Format("{0}{1}{0}", '\'', tokenValue.Token), tokenValue.RowNo, tokenValue.ColNo);
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
            throw new VistaDBSQLException(613, methodName, parser.TokenValue.RowNo, parser.TokenValue.ColNo);
          assemblyName = token2.Substring(0, length);
          methodName = token2.Substring(length + 1, token2.Length - length - 1);
          parser.SkipToken(false);
          return;
        }
      }
      throw new VistaDBSQLException(632, string.Format("{0}{1}{0}", '\'', tokenValue.Token), tokenValue.RowNo, tokenValue.ColNo);
    }

    protected override IQueryResult OnExecuteQuery()
    {
      base.OnExecuteQuery();
      TriggerAction triggerAction = 0;
      if (!insteadOf)
      {
        if (onInsert)
          triggerAction |= TriggerAction.AfterInsert;
        if (onUpdate)
          triggerAction |= TriggerAction.AfterUpdate;
        if (onDelete)
          triggerAction |= TriggerAction.AfterDelete;
      }
      Database.RegisterClrTrigger(triggerName, methodName, assemblyName, tableName, triggerAction, description);
      return null;
    }
  }
}
