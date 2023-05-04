using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class CreateTableStatement : BaseCreateTableStatement
  {
    private bool createInPrepare;
    private static int tableIdCounter;
    private bool forSync;

    public CreateTableStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
      createInPrepare = !string.IsNullOrEmpty(tableName) && tableName[0] == '#';
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      base.OnParse(connection, parser);
      if (parser.IsToken("DESCRIPTION"))
      {
        parser.SkipToken(true);
        tableDescription = parser.TokenValue.Token;
        parser.SkipToken(true);
      }
      ParseSchema(parser);
    }

    private void ParseSchema(SQLParser parser)
    {
      parser.ExpectedExpression("(");
      ParseColumns(parser);
      parser.ExpectedExpression(")");
      parser.SkipToken(false);
      if (!parser.IsToken("FOR"))
        return;
      parser.SkipToken(true);
      parser.ExpectedExpression("SYNCHRONIZATION");
      forSync = true;
      parser.SkipToken(false);
    }

    protected override VistaDBType OnPrepareQuery()
    {
      int num = (int) base.OnPrepareQuery();
      if (createInPrepare)
      {
        createInPrepare = false;
        OnExecuteQuery();
        createInPrepare = true;
      }
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      if (createInPrepare)
        return (IQueryResult) null;
      if (connection.Database == null)
        throw new VistaDBSQLException(623, string.Empty, 0, 0);
      using (IVistaDBTableSchema vistaDbTableSchema = connection.Database.NewTable(tableName))
      {
        AddColumns(vistaDbTableSchema);
        AddConstraints(vistaDbTableSchema);
        vistaDbTableSchema.Description = tableDescription;
        try
        {
          using (IVistaDBTable table = tableName[0] == '#' ? connection.Database.CreateTemporaryTable(vistaDbTableSchema) : connection.Database.CreateTable(vistaDbTableSchema, true, false))
            AddForeignKeys(table);
          if (forSync)
            connection.Database.ActivateSyncService(tableName);
        }
        catch (VistaDBException ex)
        {
          if (!ex.Contains(145L) && connection.Database.ContainsTable(tableName))
            connection.Database.DropTable(tableName);
          throw;
        }
      }
      return (IQueryResult) null;
    }

    public string TableName
    {
      get
      {
        return tableName;
      }
    }

    internal void CreateUniqueName(string paramName)
    {
      tableName = "#" + (object) '_' + tableIdCounter.ToString() + (object) '_' + paramName;
      ++tableIdCounter;
    }
  }
}
