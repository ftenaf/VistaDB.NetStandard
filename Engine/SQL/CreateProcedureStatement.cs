using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class CreateProcedureStatement : BaseCreateStatement
  {
    protected string name;
    protected IStoredProcedureInformation storedProcedure;
    private string description;
    private string assemblyName;
    private string externalName;

    internal static bool ParseExternalName(string statementName, SQLParser parser, out string assemblyName, out string externalName)
    {
      if (parser.IsToken("EXTERNAL"))
      {
        parser.ExpectedExpression("EXTERNAL");
        parser.SkipToken(true);
        parser.ExpectedExpression("NAME");
        parser.SkipToken(true);
        string token = parser.TokenValue.Token;
        parser.SkipToken(false);
        while (!parser.EndOfText && parser.TokenValue.TokenType == TokenType.Unknown && parser.TokenValue.Token[0] == '.')
        {
          token += parser.TokenValue.Token;
          parser.SkipToken(false);
        }
        externalName = token;
        int length = externalName.IndexOf('.');
        if (length <= 0)
          throw new VistaDBSQLException(613, statementName, parser.TokenValue.RowNo, parser.TokenValue.ColNo);
        assemblyName = externalName.Substring(0, length);
        if (assemblyName[0] == '[' && assemblyName[assemblyName.Length - 1] == ']')
          assemblyName = assemblyName.Substring(1, assemblyName.Length - 2);
        externalName = externalName.Substring(length + 1, externalName.Length - length - 1);
        int num = externalName.LastIndexOf('.');
        if (num > 0 && externalName[0] == '[' && externalName[num - 1] == ']')
          externalName = externalName.Substring(1, num - 2) + "." + externalName.Substring(num + 1);
        return true;
      }
      assemblyName = null;
      externalName = null;
      return false;
    }

    internal CreateProcedureStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      parser.SkipToken(true);
      parser.ParseComplexName(out name);
      parser.SkipToken(true);
      if (parser.IsToken("DESCRIPTION"))
      {
        parser.SkipToken(true);
        description = parser.TokenValue.Token;
        parser.SkipToken(true);
      }
      else
        description = null;
      bool flag = false;
      if (parser.IsToken("("))
      {
        flag = true;
        parser.SkipToken(true);
      }
      int symbolNo = parser.TokenValue.SymbolNo;
      parser.ParseVariables();
      if (flag)
      {
        parser.ExpectedExpression(")");
        parser.SkipToken(true);
      }
      parser.ExpectedExpression("AS");
      parser.SkipToken(true);
      if (ParseExternalName(name, parser, out assemblyName, out externalName))
        return;
      parser.ExpectedExpression("BEGIN");
      connection.ParseStatement(this, id);
      storedProcedure = Database.CreateStoredProcedureInstance(name, parser.Text.Substring(symbolNo, parser.SymbolNo - symbolNo), description);
    }

    protected override VistaDBType OnPrepareQuery()
    {
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      if (storedProcedure == null)
        Database.RegisterClrProcedure(name, externalName, assemblyName, description);
      else
        Database.CreateStoredProcedureObject(storedProcedure);
      return null;
    }
  }
}
