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
      this.condition = parser.NextSignature(true, true, 6);
      this.thenStatement = connection.ParseStatement((Statement) this, this.id);
      if (!parser.SkipSemicolons() && parser.IsToken("ELSE"))
      {
        parser.SkipToken(true);
        this.elseStatement = connection.ParseStatement((Statement) this, this.id);
        parser.SkipSemicolons();
      }
      else
        this.elseStatement = (Statement) null;
      this.hasDDL = this.thenStatement.HasDDLCommands || this.elseStatement != null && this.elseStatement.HasDDLCommands;
    }

    protected override VistaDBType OnPrepareQuery()
    {
      if (this.condition.Prepare() == SignatureType.Constant && this.condition.SignatureType != SignatureType.Constant)
        this.condition = (Signature) ConstantSignature.CreateSignature(this.condition.Execute(), this.parent);
      if (this.condition.DataType != VistaDBType.Bit)
        throw new VistaDBSQLException(564, "", this.condition.LineNo, this.condition.SymbolNo);
      int num1 = (int) this.thenStatement.PrepareQuery();
      if (this.elseStatement != null)
      {
        int num2 = (int) this.elseStatement.PrepareQuery();
      }
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      this.condition.SetChanged();
      IColumn column = this.condition.Execute();
      this.thenExecuted = !column.IsNull && (bool) ((IValue) column).Value;
      return this.ExecBatch(this.thenExecuted ? this.thenStatement : this.elseStatement);
    }

    private IQueryResult ExecBatch(Statement batchStatement)
    {
      if (batchStatement == null)
        return (IQueryResult) null;
      if (batchStatement is BatchStatement)
        batchStatement.DoSetReturnParameter(this.DoGetReturnParameter());
      return batchStatement.ExecuteQuery();
    }

    public override IQuerySchemaInfo GetSchemaInfo()
    {
      if (this.thenExecuted)
        return this.thenStatement.GetSchemaInfo();
      if (this.elseStatement == null)
        return (IQuerySchemaInfo) null;
      return this.elseStatement.GetSchemaInfo();
    }
  }
}
