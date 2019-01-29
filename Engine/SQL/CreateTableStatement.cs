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
      this.createInPrepare = !string.IsNullOrEmpty(this.tableName) && this.tableName[0] == '#';
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      base.OnParse(connection, parser);
      if (parser.IsToken("DESCRIPTION"))
      {
        parser.SkipToken(true);
        this.tableDescription = parser.TokenValue.Token;
        parser.SkipToken(true);
      }
      this.ParseSchema(parser);
    }

    private void ParseSchema(SQLParser parser)
    {
      parser.ExpectedExpression("(");
      this.ParseColumns(parser);
      parser.ExpectedExpression(")");
      parser.SkipToken(false);
      if (!parser.IsToken("FOR"))
        return;
      parser.SkipToken(true);
      parser.ExpectedExpression("SYNCHRONIZATION");
      this.forSync = true;
      parser.SkipToken(false);
    }

    protected override VistaDBType OnPrepareQuery()
    {
      int num = (int) base.OnPrepareQuery();
      if (this.createInPrepare)
      {
        this.createInPrepare = false;
        this.OnExecuteQuery();
        this.createInPrepare = true;
      }
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      if (this.createInPrepare)
        return (IQueryResult) null;
      if (this.connection.Database == null)
        throw new VistaDBSQLException(623, string.Empty, 0, 0);
      using (IVistaDBTableSchema vistaDbTableSchema = this.connection.Database.NewTable(this.tableName))
      {
        this.AddColumns(vistaDbTableSchema);
        this.AddConstraints(vistaDbTableSchema);
        vistaDbTableSchema.Description = this.tableDescription;
        try
        {
          using (IVistaDBTable table = this.tableName[0] == '#' ? this.connection.Database.CreateTemporaryTable(vistaDbTableSchema) : this.connection.Database.CreateTable(vistaDbTableSchema, true, false))
            this.AddForeignKeys(table);
          if (this.forSync)
            this.connection.Database.ActivateSyncService(this.tableName);
        }
        catch (VistaDBException ex)
        {
          if (!ex.Contains(145L) && this.connection.Database.ContainsTable(this.tableName))
            this.connection.Database.DropTable(this.tableName);
          throw;
        }
      }
      return (IQueryResult) null;
    }

    public string TableName
    {
      get
      {
        return this.tableName;
      }
    }

    internal void CreateUniqueName(string paramName)
    {
      this.tableName = "#" + (object) '_' + CreateTableStatement.tableIdCounter.ToString() + (object) '_' + paramName;
      ++CreateTableStatement.tableIdCounter;
    }
  }
}
