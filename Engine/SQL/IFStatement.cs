using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal class IFStatement : Statement
  {
    private Signature condition;
    private Statement thenStatement;
    private Statement elseStatement;
    private bool thenExecuted;

    public IFStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      condition = parser.NextSignature(true, true, 6);
      thenStatement = connection.ParseStatement(this, id);
      if (!parser.SkipSemicolons() && parser.IsToken("ELSE"))
      {
        parser.SkipToken(true);
        elseStatement = connection.ParseStatement(this, id);
        parser.SkipSemicolons();
      }
      else
        elseStatement = null;
      hasDDL = thenStatement.HasDDLCommands || elseStatement != null && elseStatement.HasDDLCommands;
    }

    protected override VistaDBType OnPrepareQuery()
    {
      if (condition.Prepare() == SignatureType.Constant && condition.SignatureType != SignatureType.Constant)
        condition = ConstantSignature.CreateSignature(condition.Execute(), parent);
      if (condition.DataType != VistaDBType.Bit)
        throw new VistaDBSQLException(564, "", condition.LineNo, condition.SymbolNo);
      int num1 = (int) thenStatement.PrepareQuery();
      if (elseStatement != null)
      {
        int num2 = (int) elseStatement.PrepareQuery();
      }
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      condition.SetChanged();
      IColumn column = condition.Execute();
      thenExecuted = !column.IsNull && (bool)column.Value;
      return ExecBatch(thenExecuted ? thenStatement : elseStatement);
    }

    private IQueryResult ExecBatch(Statement batchStatement)
    {
      if (batchStatement == null)
        return null;
      if (batchStatement is BatchStatement)
        batchStatement.DoSetReturnParameter(DoGetReturnParameter());
      return batchStatement.ExecuteQuery();
    }

    public override IQuerySchemaInfo GetSchemaInfo()
    {
      if (thenExecuted)
        return thenStatement.GetSchemaInfo();
      if (elseStatement == null)
        return null;
      return elseStatement.GetSchemaInfo();
    }
  }
}
