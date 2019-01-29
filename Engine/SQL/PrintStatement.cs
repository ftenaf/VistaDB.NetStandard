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
      this._message.SetChanged();
      this.connection.OnPrintMessage(this._message.Execute().ToString());
      return (IQueryResult) null;
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      this._message = parser.NextSignature(true, false, 6);
      if (this._message == (Signature) null)
        throw new VistaDBSQLException(509, "Invalid syntax near print", this.lineNo, this.symbolNo);
    }

    protected override VistaDBType OnPrepareQuery()
    {
      if (this._message.Prepare() == SignatureType.Constant && this._message.SignatureType != SignatureType.Constant)
        this._message = (Signature) ConstantSignature.CreateSignature(this._message.Execute(), this.parent);
      return VistaDBType.Unknown;
    }
  }
}
