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
          returnParameterName = signature.Text.Substring(1);
          if (parent.DoGetParam(returnParameterName) == null)
            throw new VistaDBSQLException(616, signature.Text, lineNo, symbolNo);
          signature = parser.NextSignature(true, true, 6);
        }
        else
          returnParameterName = (string) null;
        subProgramSignature = signature as ProgrammabilitySignature;
        if ((Signature) subProgramSignature == (Signature) null)
        {
          if (!connection.DatabaseOpened)
            throw new VistaDBSQLException(1012, string.Empty, 0, 0);
          throw new VistaDBSQLException(607, signature.Text, lineNo, symbolNo);
        }
      }
      while (parser.IsToken(",") && parser.SkipToken(true));
    }

    protected override VistaDBType OnPrepareQuery()
    {
      int num = (int) subProgramSignature.Prepare();
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      IParameter parameter = returnParameterName == null ? parent.DoGetReturnParameter() : parent.DoGetParam(returnParameterName);
      if (parameter != null)
        subProgramSignature.SetReturnParameter(parameter);
      subProgramSignature.Execute();
      prepared = false;
      return (IQueryResult) null;
    }

    public override void Dispose()
    {
      try
      {
        if (!((Signature) subProgramSignature != (Signature) null))
          return;
        subProgramSignature.DisposeSubProgramStatement();
      }
      finally
      {
        base.Dispose();
      }
    }
  }
}
