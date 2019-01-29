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
        throw new VistaDBSQLException(500, "\"(\"", this.lineNo, this.symbolNo);
      this.parameters.Add(parser.NextSignature(true, true, 6));
      parser.ExpectedExpression("AS");
      parser.SkipToken(true);
      this.dataType = parser.ReadDataType(out this.len);
      if (this.len == 0)
        this.len = 30;
      parser.ExpectedExpression(")");
      this.paramValues = new IColumn[1];
      this.parameterTypes = new VistaDBType[1];
      this.parameterTypes[0] = this.dataType;
      this.signatureType = SignatureType.Expression;
      this.skipNull = true;
      this.width = 0;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      this.width = !Utils.IsCharacterDataType(this.dataType) || Utils.IsCharacterDataType(this[0].DataType) ? this[0].GetWidth() : this.len;
      return signatureType;
    }

    protected override bool IsEquals(Signature signature)
    {
      if (!base.IsEquals(signature))
        return false;
      CastFunction castFunction = (CastFunction) signature;
      if (this.dataType == signature.DataType)
        return this.len == castFunction.len;
      return false;
    }

    protected override object ExecuteSubProgram()
    {
      return ((IValue) this.paramValues[0]).Value;
    }

    public override int GetWidth()
    {
      return this.width;
    }
  }
}
