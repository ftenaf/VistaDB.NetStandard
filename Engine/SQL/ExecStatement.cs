using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal class ExecStatement : Statement
  {
    private ProgrammabilitySignature subProgramSignature;
    private string returnParameterName;

    public ExecStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      do
      {
        bool needSkip = parser.IsToken("EXEC") || parser.IsToken("EXECUTE");
        Signature signature = parser.NextSignature(needSkip, true, -1);
        if (signature.SignatureType == SignatureType.Parameter)
        {
          this.returnParameterName = signature.Text.Substring(1);
          if (this.parent.DoGetParam(this.returnParameterName) == null)
            throw new VistaDBSQLException(616, signature.Text, this.lineNo, this.symbolNo);
          signature = parser.NextSignature(true, true, 6);
        }
        else
          this.returnParameterName = (string) null;
        this.subProgramSignature = signature as ProgrammabilitySignature;
        if ((Signature) this.subProgramSignature == (Signature) null)
        {
          if (!connection.DatabaseOpened)
            throw new VistaDBSQLException(1012, string.Empty, 0, 0);
          throw new VistaDBSQLException(607, signature.Text, this.lineNo, this.symbolNo);
        }
      }
      while (parser.IsToken(",") && parser.SkipToken(true));
    }

    protected override VistaDBType OnPrepareQuery()
    {
      int num = (int) this.subProgramSignature.Prepare();
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      IParameter parameter = this.returnParameterName == null ? this.parent.DoGetReturnParameter() : this.parent.DoGetParam(this.returnParameterName);
      if (parameter != null)
        this.subProgramSignature.SetReturnParameter(parameter);
      this.subProgramSignature.Execute();
      this.prepared = false;
      return (IQueryResult) null;
    }

    public override void Dispose()
    {
      try
      {
        if (!((Signature) this.subProgramSignature != (Signature) null))
          return;
        this.subProgramSignature.DisposeSubProgramStatement();
      }
      finally
      {
        base.Dispose();
      }
    }
  }
}
