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
        throw new VistaDBSQLException(660, "unsupported syntax", lineNo, symbolNo);
      if (signatureList.Count < 3)
        throw new VistaDBSQLException(501, "RAISERROR requires three parameters, message | id, severity, state", lineNo, symbolNo);
      _message = signatureList[0];
      signatureList.RemoveAt(0);
      IColumn column1 = signatureList[0].Execute();
      signatureList.RemoveAt(0);
      IColumn column2 = signatureList[0].Execute();
      signatureList.RemoveAt(0);
      _severity = (int) (long)column1.Value;
      _state = (int) (long)column2.Value;
      _arguments = signatureList.ToArray();
    }

    protected override VistaDBType OnPrepareQuery()
    {
      if (_message.Prepare() == SignatureType.Constant && _message.SignatureType != SignatureType.Constant)
        _message = ConstantSignature.CreateSignature(_message.Execute(), parent);
      for (int index = 0; index < _arguments.Length; ++index)
      {
        Signature signature = _arguments[index];
        if (signature.Prepare() == SignatureType.Constant && signature.SignatureType != SignatureType.Constant)
          _arguments[index] = ConstantSignature.CreateSignature(signature.Execute(), parent);
      }
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      _message.SetChanged();
      IColumn[] columnArray = new IColumn[_arguments.Length];
      for (int index = 0; index < _arguments.Length; ++index)
      {
        Signature signature = _arguments[index];
        signature.SetChanged();
        columnArray[index] = signature.Execute();
      }
      string str = _message.Execute().ToString();
      int result;
      if (int.TryParse(str, out result))
        connection.LastException = parent.Exception = new VistaDBSQLException(result, "Error #" + str, lineNo, symbolNo);
      else
        connection.LastException = parent.Exception = new VistaDBSQLException(50000, str, lineNo, symbolNo);
      return null;
    }
  }
}
