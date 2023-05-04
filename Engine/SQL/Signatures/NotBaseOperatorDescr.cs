using System;
using VistaDB.Diagnostic;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class NotBaseOperatorDescr : Priority3Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      parser.SkipToken(true);
      string token = parser.TokenValue.Token;
      if (string.Compare("IN", token, StringComparison.OrdinalIgnoreCase) == 0)
        return new NotInOperator(leftSignature, parser);
      if (string.Compare("LIKE", token, StringComparison.OrdinalIgnoreCase) == 0)
        return new NotLikeOperator(leftSignature, parser);
      if (string.Compare("BETWEEN", token, StringComparison.OrdinalIgnoreCase) == 0)
        return new NotBetweenOperator(leftSignature, parser);
      throw new VistaDBSQLException(507, "LIKE, IN, BETWEEN", parser.TokenValue.RowNo, parser.TokenValue.ColNo);
    }
  }
}
