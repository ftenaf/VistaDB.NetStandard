using System;
using System.Globalization;
using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class IsNumericFunction : Function
  {
    internal IsNumericFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      skipNull = false;
      parameterTypes[0] = VistaDBType.Unknown;
      dataType = VistaDBType.Int;
      result = CreateColumn(dataType);
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      Signature signature = this[0];
      if (signatureType != SignatureType.Constant && signature.DataType != dataType && signature.SignatureType == SignatureType.Constant)
        this[0] = ConstantSignature.PrepareAndCheckConstant(signature, dataType);
      return signatureType;
    }

    protected override object ExecuteSubProgram()
    {
      bool flag = false;
      IColumn column = this[0].Execute();
      if (!column.IsNull)
      {
        switch (column.Type)
        {
          case VistaDBType.Char:
          case VistaDBType.NChar:
          case VistaDBType.VarChar:
          case VistaDBType.NVarChar:
          case VistaDBType.Text:
          case VistaDBType.NText:
            long result1;
            double result2;
            Decimal result3;
            flag = long.TryParse(((IValue) column).Value.ToString(), NumberStyles.Integer, CrossConversion.NumberFormat, out result1) || double.TryParse(((IValue) column).Value.ToString(), NumberStyles.Float, CrossConversion.NumberFormat, out result2) || Decimal.TryParse(((IValue) column).Value.ToString(), NumberStyles.Number, CrossConversion.NumberFormat, out result3);
            break;
          case VistaDBType.TinyInt:
          case VistaDBType.SmallInt:
          case VistaDBType.Int:
          case VistaDBType.BigInt:
          case VistaDBType.Real:
          case VistaDBType.Float:
          case VistaDBType.Decimal:
          case VistaDBType.Money:
          case VistaDBType.SmallMoney:
            flag = true;
            break;
          default:
            flag = false;
            break;
        }
      }
      ((IValue) result).Value = (object) (flag ? 1 : 0);
      return ((IValue) result).Value;
    }
  }
}
