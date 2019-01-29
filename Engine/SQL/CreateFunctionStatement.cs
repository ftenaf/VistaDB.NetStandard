using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal class CreateFunctionStatement : BaseCreateStatement
  {
    protected string functionName;
    private string description;
    private string assemblyName;
    private string externalName;
    private VistaDBType resultType;
    protected IUserDefinedFunctionInformation function;

    internal CreateFunctionStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      bool scalarValued = false;
      parser.SkipToken(true);
      parser.ParseComplexName(out this.functionName);
      parser.SkipToken(true);
      if (parser.IsToken("DESCRIPTION"))
      {
        parser.SkipToken(true);
        this.description = parser.TokenValue.Token;
        parser.SkipToken(true);
      }
      else
        this.description = (string) null;
      int symbolNo = parser.TokenValue.SymbolNo;
      parser.ExpectedExpression("(");
      parser.SkipToken(true);
      parser.ParseVariables();
      parser.ExpectedExpression(")");
      parser.SkipToken(true);
      parser.ExpectedExpression("RETURNS");
      parser.SkipToken(true);
      if (ParameterSignature.IsParameter(parser.TokenValue.Token))
      {
        parser.SkipToken(true);
        parser.ExpectedExpression("TABLE");
        parser.SkipToken(true);
        using (new CreateTableStatement(this.connection, this.parent, parser, -1L))
          ;
      }
      else
      {
        int len = 0;
        VistaDBType vistaDbType = parser.ReadDataType(out len);
        if (vistaDbType == VistaDBType.Unknown)
        {
          this.resultType = VistaDBType.Unknown;
        }
        else
        {
          scalarValued = true;
          this.resultType = vistaDbType;
        }
      }
      parser.ExpectedExpression("AS");
      parser.SkipToken(true);
      if (CreateProcedureStatement.ParseExternalName(this.functionName, parser, out this.assemblyName, out this.externalName))
        return;
      parser.ExpectedExpression("BEGIN");
      try
      {
        parser.PushContext(new CurrentTokenContext(CurrentTokenContext.TokenContext.StoredFunction, this.functionName));
        connection.ParseStatement((Statement) this, this.id);
      }
      finally
      {
        parser.PopContext();
      }
      this.function = this.Database.CreateUserDefinedFunctionInstance(this.functionName, parser.Text.Substring(symbolNo, parser.SymbolNo - symbolNo), scalarValued, this.description);
    }

    protected override IQueryResult OnExecuteQuery()
    {
      if (this.function == null)
        this.Database.RegisterClrProcedure(this.functionName, this.externalName, this.assemblyName, this.description);
      else
        this.Database.CreateUserDefinedFunctionObject(this.function);
      return (IQueryResult) null;
    }
  }
}
