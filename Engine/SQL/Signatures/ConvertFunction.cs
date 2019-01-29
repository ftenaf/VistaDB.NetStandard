using System;
using System.Globalization;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class ConvertFunction : Function
  {
    private static readonly string[] dateFormat1 = new string[31]{ "MM/dd/yy", "yy.MM.dd", "dd/MM/yy", "dd.MM.yy", "dd-MM-yy", "dd MMM yy", "MMM dd, yy", "hh:mm:ss", "MMM dd yyyy hh:mm:ss:ffftt", "MM-dd-yy", "yy/MM/dd", "yyMMdd", "dd MMM yyyy HH:mm:ss:fff", "HH:mm:ss:fff", "HH:mm:ss:fff", "HH:mm:ss:fff", "HH:mm:ss:fff", "HH:mm:ss:fff", "HH:mm:ss:fff", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.fff", "yyyy-MM-dd HH:mm:ss.fff", "yyyy-MM-dd HH:mm:ss.fff", "yyyy-MM-dd HH:mm:ss.fff", "yyyy-MM-dd HH:mm:ss.fff", "yyyy-MM-ddThh:mm:ss.fff", "yyyy-MM-ddThh:mm:ss.fffz", "yyyy-MM-ddThh:mm:ss.fffz", "yyyy-MM-ddThh:mm:ss.fffz", "dd MMM yyyy hh:mm:ss:ffftt", "dd/MM/yy hh:mm:ss:ffftt" };
    private static readonly string[] dateFormat2 = new string[31]{ "MM/dd/yyyy", "yyyy.MM.dd", "dd/MM/yyyy", "dd.MM.yyyy", "dd-MM-yyyy", "dd MMM yyyy", "MMM dd, yyyy", "hh:mm:ss", "MMM dd yyyy hh:mm:ss:ffftt", "MM-dd-yyyy", "yyyy/MM/dd", "yyyyMMdd", "dd MMM yyyy HH:mm:ss:fff", "HH:mm:ss:fff", "HH:mm:ss:fff", "HH:mm:ss:fff", "HH:mm:ss:fff", "HH:mm:ss:fff", "HH:mm:ss:fff", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.fff", "yyyy-MM-dd HH:mm:ss.fff", "yyyy-MM-dd HH:mm:ss.fff", "yyyy-MM-dd HH:mm:ss.fff", "yyyy-MM-dd HH:mm:ss.fff", "yyyy-MM-ddThh:mm:ss.fff", "yyyy-MM-ddThh:mm:ss.fffz", "yyyy-MM-ddThh:mm:ss.fffz", "yyyy-MM-ddThh:mm:ss.fffz", "dd MMM yyyy hh:mm:ss:ffftt", "dd/MM/yy hh:mm:ss:ffftt" };
    private int width;
    private int style;
    private int len;
    private ConvertFunction.StyleType styleType;
    private int styleIndex;

    public ConvertFunction(SQLParser parser)
      : base(parser)
    {
      parser.SkipToken(true);
      if (!parser.IsToken("("))
        throw new VistaDBSQLException(500, "\"(\"", this.lineNo, this.symbolNo);
      parser.SkipToken(true);
      this.dataType = parser.ReadDataType(out this.len);
      if (this.len == 0)
        this.len = 30;
      parser.ExpectedExpression(",");
      this.parameters.Add(parser.NextSignature(true, true, 6));
      if (parser.IsToken(","))
      {
        parser.SkipToken(true);
        if (parser.TokenValue.TokenType != TokenType.Integer)
          throw new VistaDBSQLException(550, "CONVERT", this.lineNo, this.symbolNo);
        this.style = int.Parse(parser.TokenValue.Token, CrossConversion.NumberFormat);
        parser.SkipToken(true);
      }
      else
        this.style = 0;
      parser.ExpectedExpression(")");
      this.paramValues = new IColumn[1];
      this.parameterTypes = new VistaDBType[1];
      this.parameterTypes[0] = this.style == 0 ? this.dataType : VistaDBType.Unknown;
      this.signatureType = SignatureType.Expression;
      this.skipNull = true;
      this.width = 0;
      this.styleType = ConvertFunction.StyleType.None;
      this.styleIndex = -1;
      if (this.style == 0)
        return;
      if (this.style >= 1 && this.style <= ConvertFunction.dateFormat1.Length)
      {
        this.styleType = ConvertFunction.StyleType.DateWithoutCentury;
        this.styleIndex = this.style - 1;
      }
      else
      {
        if (this.style < 101 || this.style > ConvertFunction.dateFormat1.Length + 100)
          return;
        this.styleType = ConvertFunction.StyleType.DateWithCentury;
        this.styleIndex = this.style - 101;
      }
    }

    public override SignatureType OnPrepare()
    {
      this[0] = ConstantSignature.PrepareAndCheckConstant(this[0], this.parameterTypes[0]);
      if (!Utils.CompatibleTypes(this[0].DataType, this.dataType))
        throw new VistaDBSQLException(550, this.text, this.lineNo, this.symbolNo);
      this.paramValues[0] = this.CreateColumn(this[0].DataType);
      this.width = !Utils.IsCharacterDataType(this.dataType) || Utils.IsCharacterDataType(this[0].DataType) ? this[0].GetWidth() : this.len;
      if (this[0].SignatureType != SignatureType.Constant && !this.AlwaysNull)
        return this.signatureType;
      return SignatureType.Constant;
    }

    protected override bool IsEquals(Signature signature)
    {
      if (!base.IsEquals(signature))
        return false;
      ConvertFunction convertFunction = (ConvertFunction) signature;
      if (this.dataType == signature.DataType && this.len == convertFunction.len)
        return this.style == convertFunction.style;
      return false;
    }

    protected override object ExecuteSubProgram()
    {
      VistaDBType type = this.paramValues[0].Type;
      if (this.style != 0 && Utils.IsCharacterDataType(this.dataType))
      {
        if (Utils.IsDateDataType(type))
        {
          switch (this.styleType)
          {
            case ConvertFunction.StyleType.DateWithoutCentury:
              return (object) ((DateTime) ((IValue) this.paramValues[0]).Value).ToString(ConvertFunction.dateFormat1[this.styleIndex]);
            case ConvertFunction.StyleType.DateWithCentury:
              return (object) ((DateTime) ((IValue) this.paramValues[0]).Value).ToString(ConvertFunction.dateFormat2[this.styleIndex]);
          }
        }
      }
      else if (this.style != 0 && Utils.IsDateDataType(this.dataType) && Utils.IsCharacterDataType(type))
      {
        switch (this.styleType)
        {
          case ConvertFunction.StyleType.DateWithoutCentury:
            return (object) DateTime.ParseExact((string) ((IValue) this.paramValues[0]).Value, ConvertFunction.dateFormat1[this.styleIndex], (IFormatProvider) CultureInfo.InvariantCulture.DateTimeFormat);
          case ConvertFunction.StyleType.DateWithCentury:
            return (object) DateTime.ParseExact((string) ((IValue) this.paramValues[0]).Value, ConvertFunction.dateFormat2[this.styleIndex], (IFormatProvider) CultureInfo.InvariantCulture.DateTimeFormat);
        }
      }
      this.Convert((IValue) this.paramValues[0], (IValue) this.result);
      return ((IValue) this.result).Value;
    }

    public override int GetWidth()
    {
      return this.width;
    }

    private enum StyleType
    {
      None,
      DateWithoutCentury,
      DateWithCentury,
    }
  }
}
