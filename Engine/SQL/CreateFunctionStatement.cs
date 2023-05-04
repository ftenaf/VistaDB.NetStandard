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
            parser.ParseComplexName(out functionName);
            parser.SkipToken(true);
            if (parser.IsToken("DESCRIPTION"))
            {
                parser.SkipToken(true);
                description = parser.TokenValue.Token;
                parser.SkipToken(true);
            }
            else
                description = (string)null;
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
                using (new CreateTableStatement(this.connection, parent, parser, -1L)) ;
            }
            else
            {
                int len = 0;
                VistaDBType vistaDbType = parser.ReadDataType(out len);
                if (vistaDbType == VistaDBType.Unknown)
                {
                    resultType = VistaDBType.Unknown;
                }
                else
                {
                    scalarValued = true;
                    resultType = vistaDbType;
                }
            }
            parser.ExpectedExpression("AS");
            parser.SkipToken(true);
            if (CreateProcedureStatement.ParseExternalName(functionName, parser, out assemblyName, out externalName))
                return;
            parser.ExpectedExpression("BEGIN");
            try
            {
                parser.PushContext(new CurrentTokenContext(CurrentTokenContext.TokenContext.StoredFunction, functionName));
                connection.ParseStatement((Statement)this, id);
            }
            finally
            {
                parser.PopContext();
            }
            function = Database.CreateUserDefinedFunctionInstance(functionName, parser.Text.Substring(symbolNo, parser.SymbolNo - symbolNo), scalarValued, description);
        }

        protected override IQueryResult OnExecuteQuery()
        {
            if (function == null)
                Database.RegisterClrProcedure(functionName, externalName, assemblyName, description);
            else
                Database.CreateUserDefinedFunctionObject(function);
            return (IQueryResult)null;
        }
    }
}
