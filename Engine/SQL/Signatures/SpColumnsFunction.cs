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
      SpColumnsFunction.typePrecisionMap.Add(11, 19);
      SpColumnsFunction.typePrecisionMap.Add(17, 1);
      SpColumnsFunction.typePrecisionMap.Add(1, -1);
      SpColumnsFunction.typePrecisionMap.Add(19, 23);
      SpColumnsFunction.typePrecisionMap.Add(14, 18);
      SpColumnsFunction.typePrecisionMap.Add(13, 15);
      SpColumnsFunction.typePrecisionMap.Add(20, int.MaxValue);
      SpColumnsFunction.typePrecisionMap.Add(10, 10);
      SpColumnsFunction.typePrecisionMap.Add(15, 19);
      SpColumnsFunction.typePrecisionMap.Add(2, -1);
      SpColumnsFunction.typePrecisionMap.Add(6, 1073741823);
      SpColumnsFunction.typePrecisionMap.Add(4, -1);
      SpColumnsFunction.typePrecisionMap.Add(12, 7);
      SpColumnsFunction.typePrecisionMap.Add(23, 16);
      SpColumnsFunction.typePrecisionMap.Add(9, 5);
      SpColumnsFunction.typePrecisionMap.Add(16, 10);
      SpColumnsFunction.typePrecisionMap.Add(5, int.MaxValue);
      SpColumnsFunction.typePrecisionMap.Add(24, 8);
      SpColumnsFunction.typePrecisionMap.Add(8, 3);
      SpColumnsFunction.typePrecisionMap.Add(22, 36);
      SpColumnsFunction.typePrecisionMap.Add(21, -1);
      SpColumnsFunction.typePrecisionMap.Add(3, -1);
    }

    public SpColumnsFunction(SQLParser parser)
      : base(parser, 1, 19)
    {
      if (this.ParamCount > 1)
        throw new VistaDBSQLException(501, "SP_COLUMNS", this.lineNo, this.symbolNo);
      if (this.ParamCount == 1)
        this.parameterTypes[0] = VistaDBType.NChar;
      this.resultColumnTypes[0] = VistaDBType.NVarChar;
      this.resultColumnTypes[1] = VistaDBType.NVarChar;
      this.resultColumnTypes[2] = VistaDBType.NVarChar;
      this.resultColumnTypes[3] = VistaDBType.NVarChar;
      this.resultColumnTypes[4] = VistaDBType.SmallInt;
      this.resultColumnTypes[5] = VistaDBType.NVarChar;
      this.resultColumnTypes[6] = VistaDBType.Int;
      this.resultColumnTypes[7] = VistaDBType.Int;
      this.resultColumnTypes[8] = VistaDBType.SmallInt;
      this.resultColumnTypes[9] = VistaDBType.SmallInt;
      this.resultColumnTypes[10] = VistaDBType.Bit;
      this.resultColumnTypes[11] = VistaDBType.NVarChar;
      this.resultColumnTypes[12] = VistaDBType.NVarChar;
      this.resultColumnTypes[13] = VistaDBType.SmallInt;
      this.resultColumnTypes[14] = VistaDBType.SmallInt;
      this.resultColumnTypes[15] = VistaDBType.Int;
      this.resultColumnTypes[16] = VistaDBType.Int;
      this.resultColumnTypes[17] = VistaDBType.NVarChar;
      this.resultColumnTypes[18] = VistaDBType.TinyInt;
      this.resultColumnNames[0] = "TABLE_QUALIFIER";
      this.resultColumnNames[1] = "TABLE_OWNER";
      this.resultColumnNames[2] = "TABLE_NAME";
      this.resultColumnNames[3] = "COLUMN_NAME";
      this.resultColumnNames[4] = "DATA_TYPE";
      this.resultColumnNames[5] = "TYPE_NAME";
      this.resultColumnNames[6] = "PRECISION";
      this.resultColumnNames[7] = "LENGTH";
      this.resultColumnNames[8] = "SCALE";
      this.resultColumnNames[9] = "RADIX";
      this.resultColumnNames[10] = "NULLABLE";
      this.resultColumnNames[11] = "REMARKS";
      this.resultColumnNames[12] = "COLUMN_DEF";
      this.resultColumnNames[13] = "SQL_DATA_TYPE";
      this.resultColumnNames[14] = "SQL_DATETIME_SUB";
      this.resultColumnNames[15] = "CHAR_OCTET_LENGTH";
      this.resultColumnNames[16] = "ORDINAL_POSITION";
      this.resultColumnNames[17] = "IS_NULLABLE";
      this.resultColumnNames[18] = "SS_DATA_TYPE";
    }

    public override bool GetNextResult(IRow row)
    {
      if (!this.enumerator.MoveNext())
        return false;
      this.FillRow(row);
      return true;
    }

    public override void Close()
    {
    }

    protected override object ExecuteSubProgram()
    {
      string str = ((IValue) this.paramValues[0]).Value as string;
      this.schema = (IVistaDBTableSchema) null;
      try
      {
        this.schema = this.parent.Database.TableSchema(str);
      }
      catch (VistaDBException ex)
      {
        throw new VistaDBSQLException((Exception) ex, 572, str, this.lineNo, this.symbolNo);
      }
      catch
      {
        throw;
      }
      this.enumerator = (IEnumerator) this.schema.GetEnumerator();
      return (object) null;
    }

    public override bool First(IRow row)
    {
      this.enumerator.Reset();
      if (!this.enumerator.MoveNext())
        return false;
      this.FillRow(row);
      return true;
    }

    private static int GetVistaDBTypePrecision(VistaDBType columnType, int columnLength)
    {
      int num = SpColumnsFunction.typePrecisionMap[(int) columnType];
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
      IVistaDBColumnAttributes current = this.enumerator.Current as IVistaDBColumnAttributes;
      if (current == null)
        return;
      ((IValue) row[0]).Value = (object) Path.GetFileNameWithoutExtension(this.parent.Database.Name);
      ((IValue) row[1]).Value = (object) (91.ToString() + "DBO" + (object) ']');
      ((IValue) row[2]).Value = (object) this.schema.Name;
      ((IValue) row[3]).Value = (object) current.Name;
      ((IValue) row[4]).Value = (object) (short) current.Type;
      ((IValue) row[5]).Value = (object) current.Type.ToString();
      ((IValue) row[6]).Value = (object) SpColumnsFunction.GetVistaDBTypePrecision(current.Type, current.MaxLength);
      ((IValue) row[7]).Value = (object) current.MaxLength;
      short vistaDbTypeScale = SpColumnsFunction.GetVistaDBTypeScale(current.Type);
      ((IValue) row[8]).Value = vistaDbTypeScale < (short) 0 ? (object) null : (object) vistaDbTypeScale;
      short vistaDbTypeRadix = SpColumnsFunction.GetVistaDBTypeRadix(current.Type);
      ((IValue) row[9]).Value = vistaDbTypeRadix < (short) 0 ? (object) null : (object) vistaDbTypeRadix;
      ((IValue) row[10]).Value = (object) current.AllowNull;
      ((IValue) row[11]).Value = (object) current.Description;
      IVistaDBDefaultValueInformation defaultValue = this.schema.DefaultValues[current.Name];
      ((IValue) row[12]).Value = defaultValue == null ? (object) (string) null : (object) defaultValue.Expression;
      ((IValue) row[13]).Value = (object) (short) current.Type;
      short dateTimeSub = SpColumnsFunction.GetDateTimeSub(current.Type);
      ((IValue) row[14]).Value = dateTimeSub < (short) 0 ? (object) null : (object) dateTimeSub;
      int charOctetLength = SpColumnsFunction.GetCharOctetLength(current.Type, current.MaxLength);
      ((IValue) row[15]).Value = charOctetLength < 0 ? (object) null : (object) charOctetLength;
      ((IValue) row[16]).Value = (object) (current.RowIndex + 1);
      ((IValue) row[17]).Value = current.SystemType == typeof (string) ? (object) "YES" : (object) "NO";
    }
  }
}
