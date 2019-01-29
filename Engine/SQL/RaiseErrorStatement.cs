using System.Collections.Generic;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal class RaiseErrorStatement : Statement
  {
    private Signature _message;
    private int _severity;
    private int _state;
    private Signature[] _arguments;

    internal RaiseErrorStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      List<Signature> signatureList = new List<Signature>();
      parser.SkipToken(false);
      if (parser.IsToken("("))
        parser.SkipToken(true);
      while (!parser.IsToken(";") && !parser.EndOfText)
      {
        if (ParameterSignature.IsParameter(parser.TokenValue.Token))
          signatureList.Add(parser.NextSignature(false, true, -1));
        else
          signatureList.Add(parser.NextSignature(false, true, 6));
        if (!parser.IsToken(",") || !parser.SkipToken(true))
          break;
      }
      if (parser.IsToken(")"))
        parser.SkipToken(true);
      if (parser.IsToken("WITH"))
        throw new VistaDBSQLException(660, "unsupported syntax", this.lineNo, this.symbolNo);
      if (signatureList.Count < 3)
        throw new VistaDBSQLException(501, "RAISERROR requires three parameters, message | id, severity, state", this.lineNo, this.symbolNo);
      this._message = signatureList[0];
      signatureList.RemoveAt(0);
      IColumn column1 = signatureList[0].Execute();
      signatureList.RemoveAt(0);
      IColumn column2 = signatureList[0].Execute();
      signatureList.RemoveAt(0);
      this._severity = (int) (long) ((IValue) column1).Value;
      this._state = (int) (long) ((IValue) column2).Value;
      this._arguments = signatureList.ToArray();
    }

    protected override VistaDBType OnPrepareQuery()
    {
      if (this._message.Prepare() == SignatureType.Constant && this._message.SignatureType != SignatureType.Constant)
        this._message = (Signature) ConstantSignature.CreateSignature(this._message.Execute(), this.parent);
      for (int index = 0; index < this._arguments.Length; ++index)
      {
        Signature signature = this._arguments[index];
        if (signature.Prepare() == SignatureType.Constant && signature.SignatureType != SignatureType.Constant)
          this._arguments[index] = (Signature) ConstantSignature.CreateSignature(signature.Execute(), this.parent);
      }
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      this._message.SetChanged();
      IColumn[] columnArray = new IColumn[this._arguments.Length];
      for (int index = 0; index < this._arguments.Length; ++index)
      {
        Signature signature = this._arguments[index];
        signature.SetChanged();
        columnArray[index] = signature.Execute();
      }
      string str = this._message.Execute().ToString();
      int result;
      if (int.TryParse(str, out result))
        this.connection.LastException = this.parent.Exception = (VistaDBException) new VistaDBSQLException(result, "Error #" + str, this.lineNo, this.symbolNo);
      else
        this.connection.LastException = this.parent.Exception = (VistaDBException) new VistaDBSQLException(50000, str, this.lineNo, this.symbolNo);
      return (IQueryResult) null;
    }
  }
}
