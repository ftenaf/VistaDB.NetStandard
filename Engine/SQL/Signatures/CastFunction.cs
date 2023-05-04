using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class CastFunction : Function
  {
    private int len;
    private int width;

    public CastFunction(SQLParser parser)
      : base(parser)
    {
      parser.SkipToken(true);
      if (!parser.IsToken("("))
        throw new VistaDBSQLException(500, "\"(\"", lineNo, symbolNo);
      parameters.Add(parser.NextSignature(true, true, 6));
      parser.ExpectedExpression("AS");
      parser.SkipToken(true);
      dataType = parser.ReadDataType(out len);
      if (len == 0)
        len = 30;
      parser.ExpectedExpression(")");
      paramValues = new IColumn[1];
      parameterTypes = new VistaDBType[1];
      parameterTypes[0] = dataType;
      signatureType = SignatureType.Expression;
      skipNull = true;
      width = 0;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      width = !Utils.IsCharacterDataType(dataType) || Utils.IsCharacterDataType(this[0].DataType) ? this[0].GetWidth() : len;
      return signatureType;
    }

    protected override bool IsEquals(Signature signature)
    {
      if (!base.IsEquals(signature))
        return false;
      CastFunction castFunction = (CastFunction) signature;
      if (dataType == signature.DataType)
        return len == castFunction.len;
      return false;
    }

    protected override object ExecuteSubProgram()
    {
      return ((IValue) paramValues[0]).Value;
    }

    public override int GetWidth()
    {
      return width;
    }
  }
}
