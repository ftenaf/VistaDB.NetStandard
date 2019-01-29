using System.Globalization;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class CreateDatabaseStatement : BaseCreateStatement
  {
    private int lcid = CultureInfo.CurrentCulture.LCID;
    private bool caseSensitive = true;
    private string fileName;
    private string cryptoKeyString;
    private int pageSize;
    private string description;
    private bool isolatedStorage;

    public CreateDatabaseStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
      this.lcid = connection.LCID;
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      if (parser.IsToken("INMEMORY"))
      {
        this.fileName = (string) null;
        parser.SkipToken(true);
        parser.ExpectedExpression("DATABASE");
      }
      else
      {
        parser.SkipToken(true);
        this.fileName = parser.TokenValue.Token;
        if (this.fileName.IndexOf(".") < 0)
          this.fileName += ".vdb4";
      }
      if (!parser.SkipToken(false))
        return;
      this.ParseParameters(parser);
      connection.LCID = this.lcid;
    }

    private void ParseParameters(SQLParser parser)
    {
      SQLParser.TokenValueClass tokenValue = parser.TokenValue;
      if (parser.IsToken("IN"))
      {
        parser.SkipToken(true);
        parser.ExpectedExpression("ISOLATED");
        parser.SkipToken(true);
        parser.ExpectedExpression("STORAGE");
        parser.SkipToken(false);
        this.isolatedStorage = true;
      }
      while (parser.IsToken(","))
      {
        parser.SkipToken(true);
        if (parser.IsToken("PASSWORD"))
        {
          parser.SkipToken(true);
          this.cryptoKeyString = tokenValue.Token;
        }
        else if (parser.IsToken("PAGE"))
        {
          parser.SkipToken(true);
          parser.ExpectedExpression("SIZE");
          parser.SkipToken(true);
          this.pageSize = BaseCreateStatement.StrTokenToInt(parser);
        }
        else if (parser.IsToken("LCID"))
        {
          parser.SkipToken(true);
          this.lcid = BaseCreateStatement.StrTokenToInt(parser);
        }
        else if (parser.IsToken("CASE"))
        {
          parser.SkipToken(true);
          parser.ExpectedExpression("SENSITIVE");
          parser.SkipToken(true);
          if (parser.IsToken("TRUE"))
          {
            this.caseSensitive = true;
          }
          else
          {
            if (!parser.IsToken("FALSE"))
              throw new VistaDBSQLException(593, "", this.lineNo, this.symbolNo);
            this.caseSensitive = false;
          }
        }
        else
        {
          if (!parser.IsToken("DESCRIPTION"))
            throw new VistaDBSQLException(593, "", this.lineNo, this.symbolNo);
          parser.SkipToken(true);
          this.description = parser.TokenValue.Token;
        }
        parser.SkipToken(false);
      }
    }

    protected override IQueryResult OnExecuteQuery()
    {
      base.OnExecuteQuery();
      if (this.connection.Database != null)
        this.connection.CloseExternalDatabase();
      if (this.fileName != null)
      {
        using (IVistaDBDDA vistaDbdda = this.connection.ParentEngine.OpenDDA())
        {
          using (IVistaDBDatabase vistaDbDatabase = this.isolatedStorage ? vistaDbdda.CreateIsolatedDatabase(this.fileName, this.cryptoKeyString, this.pageSize, this.lcid, this.caseSensitive) : vistaDbdda.CreateDatabase(this.fileName, true, this.cryptoKeyString, this.pageSize, this.lcid, this.caseSensitive))
            vistaDbDatabase.Description = this.description;
        }
        this.connection.OpenDatabase(this.fileName, VistaDBDatabaseOpenMode.ExclusiveReadWrite, this.cryptoKeyString, this.isolatedStorage);
      }
      else
        this.connection.OpenInMemoryDatabase(this.cryptoKeyString, this.lcid, this.caseSensitive);
      return (IQueryResult) null;
    }
  }
}
