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
    private StyleType styleType;
    private int styleIndex;

    public ConvertFunction(SQLParser parser)
      : base(parser)
    {
      parser.SkipToken(true);
      if (!parser.IsToken("("))
        throw new VistaDBSQLException(500, "\"(\"", lineNo, symbolNo);
      parser.SkipToken(true);
      dataType = parser.ReadDataType(out len);
      if (len == 0)
        len = 30;
      parser.ExpectedExpression(",");
      parameters.Add(parser.NextSignature(true, true, 6));
      if (parser.IsToken(","))
      {
        parser.SkipToken(true);
        if (parser.TokenValue.TokenType != TokenType.Integer)
          throw new VistaDBSQLException(550, "CONVERT", lineNo, symbolNo);
        style = int.Parse(parser.TokenValue.Token, CrossConversion.NumberFormat);
        parser.SkipToken(true);
      }
      else
        style = 0;
      parser.ExpectedExpression(")");
      paramValues = new IColumn[1];
      parameterTypes = new VistaDBType[1];
      parameterTypes[0] = style == 0 ? dataType : VistaDBType.Unknown;
      signatureType = SignatureType.Expression;
      skipNull = true;
      width = 0;
      styleType = StyleType.None;
      styleIndex = -1;
      if (style == 0)
        return;
      if (style >= 1 && style <= dateFormat1.Length)
      {
        styleType = StyleType.DateWithoutCentury;
        styleIndex = style - 1;
      }
      else
      {
        if (style < 101 || style > dateFormat1.Length + 100)
          return;
        styleType = StyleType.DateWithCentury;
        styleIndex = style - 101;
      }
    }

    public override SignatureType OnPrepare()
    {
      this[0] = ConstantSignature.PrepareAndCheckConstant(this[0], parameterTypes[0]);
      if (!Utils.CompatibleTypes(this[0].DataType, dataType))
        throw new VistaDBSQLException(550, text, lineNo, symbolNo);
      paramValues[0] = CreateColumn(this[0].DataType);
      width = !Utils.IsCharacterDataType(dataType) || Utils.IsCharacterDataType(this[0].DataType) ? this[0].GetWidth() : len;
      if (this[0].SignatureType != SignatureType.Constant && !AlwaysNull)
        return signatureType;
      return SignatureType.Constant;
    }

    protected override bool IsEquals(Signature signature)
    {
      if (!base.IsEquals(signature))
        return false;
      ConvertFunction convertFunction = (ConvertFunction) signature;
      if (dataType == signature.DataType && len == convertFunction.len)
        return style == convertFunction.style;
      return false;
    }

    protected override object ExecuteSubProgram()
    {
      VistaDBType type = paramValues[0].Type;
      if (style != 0 && Utils.IsCharacterDataType(dataType))
      {
        if (Utils.IsDateDataType(type))
        {
          switch (styleType)
          {
            case StyleType.DateWithoutCentury:
              return (object) ((DateTime) ((IValue) paramValues[0]).Value).ToString(dateFormat1[styleIndex]);
            case StyleType.DateWithCentury:
              return (object) ((DateTime) ((IValue) paramValues[0]).Value).ToString(dateFormat2[styleIndex]);
          }
        }
      }
      else if (style != 0 && Utils.IsDateDataType(dataType) && Utils.IsCharacterDataType(type))
      {
        switch (styleType)
        {
          case StyleType.DateWithoutCentury:
            return (object) DateTime.ParseExact((string) ((IValue) paramValues[0]).Value, dateFormat1[styleIndex], (IFormatProvider) CultureInfo.InvariantCulture.DateTimeFormat);
          case StyleType.DateWithCentury:
            return (object) DateTime.ParseExact((string) ((IValue) paramValues[0]).Value, dateFormat2[styleIndex], (IFormatProvider) CultureInfo.InvariantCulture.DateTimeFormat);
        }
      }
      Convert((IValue) paramValues[0], (IValue) result);
      return ((IValue) result).Value;
    }

    public override int GetWidth()
    {
      return width;
    }

    private enum StyleType
    {
      None,
      DateWithoutCentury,
      DateWithCentury,
    }
  }
}
