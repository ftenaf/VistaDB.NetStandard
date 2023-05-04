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
      lcid = connection.LCID;
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      if (parser.IsToken("INMEMORY"))
      {
        fileName = (string) null;
        parser.SkipToken(true);
        parser.ExpectedExpression("DATABASE");
      }
      else
      {
        parser.SkipToken(true);
        fileName = parser.TokenValue.Token;
        if (fileName.IndexOf(".") < 0)
          fileName += ".vdb4";
      }
      if (!parser.SkipToken(false))
        return;
      ParseParameters(parser);
      connection.LCID = lcid;
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
        isolatedStorage = true;
      }
      while (parser.IsToken(","))
      {
        parser.SkipToken(true);
        if (parser.IsToken("PASSWORD"))
        {
          parser.SkipToken(true);
          cryptoKeyString = tokenValue.Token;
        }
        else if (parser.IsToken("PAGE"))
        {
          parser.SkipToken(true);
          parser.ExpectedExpression("SIZE");
          parser.SkipToken(true);
          pageSize = StrTokenToInt(parser);
        }
        else if (parser.IsToken("LCID"))
        {
          parser.SkipToken(true);
          lcid = StrTokenToInt(parser);
        }
        else if (parser.IsToken("CASE"))
        {
          parser.SkipToken(true);
          parser.ExpectedExpression("SENSITIVE");
          parser.SkipToken(true);
          if (parser.IsToken("TRUE"))
          {
            caseSensitive = true;
          }
          else
          {
            if (!parser.IsToken("FALSE"))
              throw new VistaDBSQLException(593, "", lineNo, symbolNo);
            caseSensitive = false;
          }
        }
        else
        {
          if (!parser.IsToken("DESCRIPTION"))
            throw new VistaDBSQLException(593, "", lineNo, symbolNo);
          parser.SkipToken(true);
          description = parser.TokenValue.Token;
        }
        parser.SkipToken(false);
      }
    }

    protected override IQueryResult OnExecuteQuery()
    {
      base.OnExecuteQuery();
      if (connection.Database != null)
        connection.CloseExternalDatabase();
      if (fileName != null)
      {
        using (IVistaDBDDA vistaDbdda = connection.ParentEngine.OpenDDA())
        {
          using (IVistaDBDatabase vistaDbDatabase = isolatedStorage ? vistaDbdda.CreateIsolatedDatabase(fileName, cryptoKeyString, pageSize, lcid, caseSensitive) : vistaDbdda.CreateDatabase(fileName, true, cryptoKeyString, pageSize, lcid, caseSensitive))
            vistaDbDatabase.Description = description;
        }
        connection.OpenDatabase(fileName, VistaDBDatabaseOpenMode.ExclusiveReadWrite, cryptoKeyString, isolatedStorage);
      }
      else
        connection.OpenInMemoryDatabase(cryptoKeyString, lcid, caseSensitive);
      return (IQueryResult) null;
    }
  }
}
