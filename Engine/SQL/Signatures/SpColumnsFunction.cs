using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class SpColumnsFunction : SpecialFunction
  {
    private static Dictionary<int, int> typePrecisionMap = new Dictionary<int, int>(25);
    private IVistaDBTableSchema schema;

    static SpColumnsFunction()
    {
            typePrecisionMap.Add(11, 19);
            typePrecisionMap.Add(17, 1);
            typePrecisionMap.Add(1, -1);
            typePrecisionMap.Add(19, 23);
            typePrecisionMap.Add(14, 18);
            typePrecisionMap.Add(13, 15);
            typePrecisionMap.Add(20, int.MaxValue);
            typePrecisionMap.Add(10, 10);
            typePrecisionMap.Add(15, 19);
            typePrecisionMap.Add(2, -1);
            typePrecisionMap.Add(6, 1073741823);
            typePrecisionMap.Add(4, -1);
            typePrecisionMap.Add(12, 7);
            typePrecisionMap.Add(23, 16);
            typePrecisionMap.Add(9, 5);
            typePrecisionMap.Add(16, 10);
            typePrecisionMap.Add(5, int.MaxValue);
            typePrecisionMap.Add(24, 8);
            typePrecisionMap.Add(8, 3);
            typePrecisionMap.Add(22, 36);
            typePrecisionMap.Add(21, -1);
            typePrecisionMap.Add(3, -1);
    }

    public SpColumnsFunction(SQLParser parser)
      : base(parser, 1, 19)
    {
      if (ParamCount > 1)
        throw new VistaDBSQLException(501, "SP_COLUMNS", lineNo, symbolNo);
      if (ParamCount == 1)
        parameterTypes[0] = VistaDBType.NChar;
      resultColumnTypes[0] = VistaDBType.NVarChar;
      resultColumnTypes[1] = VistaDBType.NVarChar;
      resultColumnTypes[2] = VistaDBType.NVarChar;
      resultColumnTypes[3] = VistaDBType.NVarChar;
      resultColumnTypes[4] = VistaDBType.SmallInt;
      resultColumnTypes[5] = VistaDBType.NVarChar;
      resultColumnTypes[6] = VistaDBType.Int;
      resultColumnTypes[7] = VistaDBType.Int;
      resultColumnTypes[8] = VistaDBType.SmallInt;
      resultColumnTypes[9] = VistaDBType.SmallInt;
      resultColumnTypes[10] = VistaDBType.Bit;
      resultColumnTypes[11] = VistaDBType.NVarChar;
      resultColumnTypes[12] = VistaDBType.NVarChar;
      resultColumnTypes[13] = VistaDBType.SmallInt;
      resultColumnTypes[14] = VistaDBType.SmallInt;
      resultColumnTypes[15] = VistaDBType.Int;
      resultColumnTypes[16] = VistaDBType.Int;
      resultColumnTypes[17] = VistaDBType.NVarChar;
      resultColumnTypes[18] = VistaDBType.TinyInt;
      resultColumnNames[0] = "TABLE_QUALIFIER";
      resultColumnNames[1] = "TABLE_OWNER";
      resultColumnNames[2] = "TABLE_NAME";
      resultColumnNames[3] = "COLUMN_NAME";
      resultColumnNames[4] = "DATA_TYPE";
      resultColumnNames[5] = "TYPE_NAME";
      resultColumnNames[6] = "PRECISION";
      resultColumnNames[7] = "LENGTH";
      resultColumnNames[8] = "SCALE";
      resultColumnNames[9] = "RADIX";
      resultColumnNames[10] = "NULLABLE";
      resultColumnNames[11] = "REMARKS";
      resultColumnNames[12] = "COLUMN_DEF";
      resultColumnNames[13] = "SQL_DATA_TYPE";
      resultColumnNames[14] = "SQL_DATETIME_SUB";
      resultColumnNames[15] = "CHAR_OCTET_LENGTH";
      resultColumnNames[16] = "ORDINAL_POSITION";
      resultColumnNames[17] = "IS_NULLABLE";
      resultColumnNames[18] = "SS_DATA_TYPE";
    }

    public override bool GetNextResult(IRow row)
    {
      if (!enumerator.MoveNext())
        return false;
      FillRow(row);
      return true;
    }

    public override void Close()
    {
    }

    protected override object ExecuteSubProgram()
    {
      string str = ((IValue) paramValues[0]).Value as string;
      schema = (IVistaDBTableSchema) null;
      try
      {
        schema = parent.Database.TableSchema(str);
      }
      catch (VistaDBException ex)
      {
        throw new VistaDBSQLException((Exception) ex, 572, str, lineNo, symbolNo);
      }
      catch
      {
        throw;
      }
      enumerator = (IEnumerator) schema.GetEnumerator();
      return (object) null;
    }

    public override bool First(IRow row)
    {
      enumerator.Reset();
      if (!enumerator.MoveNext())
        return false;
      FillRow(row);
      return true;
    }

    private static int GetVistaDBTypePrecision(VistaDBType columnType, int columnLength)
    {
      int num = typePrecisionMap[(int) columnType];
      if (num < 0)
        num = columnType == VistaDBType.Char || columnType == VistaDBType.VarBinary || columnType == VistaDBType.VarChar ? columnLength : columnLength / 2;
      return num;
    }

    private static short GetVistaDBTypeScale(VistaDBType columnType)
    {
      switch (columnType)
      {
        case VistaDBType.TinyInt:
        case VistaDBType.SmallInt:
        case VistaDBType.Int:
        case VistaDBType.BigInt:
        case VistaDBType.Decimal:
        case VistaDBType.SmallDateTime:
          return 0;
        case VistaDBType.Money:
        case VistaDBType.SmallMoney:
          return 4;
        case VistaDBType.DateTime:
          return 3;
        default:
          return -1;
      }
    }

    private static int GetCharOctetLength(VistaDBType columnType, int columnLength)
    {
      switch (columnType)
      {
        case VistaDBType.Char:
        case VistaDBType.NChar:
        case VistaDBType.VarChar:
        case VistaDBType.NVarChar:
        case VistaDBType.Text:
        case VistaDBType.NText:
        case VistaDBType.Image:
        case VistaDBType.VarBinary:
        case VistaDBType.Timestamp:
          return columnLength;
        default:
          return -1;
      }
    }

    private static short GetVistaDBTypeRadix(VistaDBType columnType)
    {
      switch (columnType)
      {
        case VistaDBType.TinyInt:
        case VistaDBType.SmallInt:
        case VistaDBType.Int:
        case VistaDBType.BigInt:
        case VistaDBType.Real:
        case VistaDBType.Float:
        case VistaDBType.Decimal:
        case VistaDBType.Money:
        case VistaDBType.SmallMoney:
          return 10;
        default:
          return -1;
      }
    }

    private static short GetDateTimeSub(VistaDBType columnType)
    {
      return columnType != VistaDBType.DateTime && columnType != VistaDBType.SmallDateTime ? (short) -1 : (short) 3;
    }

    private void FillRow(IRow row)
    {
      IVistaDBColumnAttributes current = enumerator.Current as IVistaDBColumnAttributes;
      if (current == null)
        return;
      ((IValue) row[0]).Value = (object) Path.GetFileNameWithoutExtension(parent.Database.Name);
      ((IValue) row[1]).Value = (object) (91.ToString() + "DBO" + (object) ']');
      ((IValue) row[2]).Value = (object) schema.Name;
      ((IValue) row[3]).Value = (object) current.Name;
      ((IValue) row[4]).Value = (object) (short) current.Type;
      ((IValue) row[5]).Value = (object) current.Type.ToString();
      ((IValue) row[6]).Value = (object)GetVistaDBTypePrecision(current.Type, current.MaxLength);
      ((IValue) row[7]).Value = (object) current.MaxLength;
      short vistaDbTypeScale = GetVistaDBTypeScale(current.Type);
      ((IValue) row[8]).Value = vistaDbTypeScale < (short) 0 ? (object) null : (object) vistaDbTypeScale;
      short vistaDbTypeRadix = GetVistaDBTypeRadix(current.Type);
      ((IValue) row[9]).Value = vistaDbTypeRadix < (short) 0 ? (object) null : (object) vistaDbTypeRadix;
      ((IValue) row[10]).Value = (object) current.AllowNull;
      ((IValue) row[11]).Value = (object) current.Description;
      IVistaDBDefaultValueInformation defaultValue = schema.DefaultValues[current.Name];
      ((IValue) row[12]).Value = defaultValue == null ? (object) (string) null : (object) defaultValue.Expression;
      ((IValue) row[13]).Value = (object) (short) current.Type;
      short dateTimeSub = GetDateTimeSub(current.Type);
      ((IValue) row[14]).Value = dateTimeSub < (short) 0 ? (object) null : (object) dateTimeSub;
      int charOctetLength = GetCharOctetLength(current.Type, current.MaxLength);
      ((IValue) row[15]).Value = charOctetLength < 0 ? (object) null : (object) charOctetLength;
      ((IValue) row[16]).Value = (object) (current.RowIndex + 1);
      ((IValue) row[17]).Value = current.SystemType == typeof (string) ? (object) "YES" : (object) "NO";
    }
  }
}
