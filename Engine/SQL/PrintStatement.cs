using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal class PrintStatement : Statement
  {
    private Signature _message;

    internal PrintStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override IQueryResult OnExecuteQuery()
    {
      _message.SetChanged();
      connection.OnPrintMessage(_message.Execute().ToString());
      return null;
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      _message = parser.NextSignature(true, false, 6);
      if (_message == null)
        throw new VistaDBSQLException(509, "Invalid syntax near print", lineNo, symbolNo);
    }

    protected override VistaDBType OnPrepareQuery()
    {
      if (_message.Prepare() == SignatureType.Constant && _message.SignatureType != SignatureType.Constant)
        _message = ConstantSignature.CreateSignature(_message.Execute(), parent);
      return VistaDBType.Unknown;
    }
  }
}
