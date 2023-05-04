using System;
using System.Collections;
using System.IO;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class SpIndexesFuncion : SpecialFunction
  {
    private IVistaDBTableSchema schema;
    private string tableName;
    private int keyColumnIndex;

    public SpIndexesFuncion(SQLParser parser)
      : base(parser, 1, 13)
    {
      keyColumnIndex = -1;
      if (ParamCount > 1)
        throw new VistaDBSQLException(501, "SP_INDEXES", lineNo, symbolNo);
      if (ParamCount == 1)
        parameterTypes[0] = VistaDBType.NChar;
      resultColumnTypes[0] = VistaDBType.NVarChar;
      resultColumnTypes[1] = VistaDBType.NVarChar;
      resultColumnTypes[2] = VistaDBType.NVarChar;
      resultColumnTypes[3] = VistaDBType.SmallInt;
      resultColumnTypes[4] = VistaDBType.NVarChar;
      resultColumnTypes[5] = VistaDBType.NVarChar;
      resultColumnTypes[6] = VistaDBType.SmallInt;
      resultColumnTypes[7] = VistaDBType.Int;
      resultColumnTypes[8] = VistaDBType.NVarChar;
      resultColumnTypes[9] = VistaDBType.VarChar;
      resultColumnTypes[10] = VistaDBType.Int;
      resultColumnTypes[11] = VistaDBType.Int;
      resultColumnTypes[12] = VistaDBType.NVarChar;
      resultColumnNames[0] = "TABLE_CAT";
      resultColumnNames[1] = "TABLE_SCHEM";
      resultColumnNames[2] = "TABLE_NAME";
      resultColumnNames[3] = "NON_UNIQUE";
      resultColumnNames[4] = "INDEX_QUALIFER";
      resultColumnNames[5] = "INDEX_NAME";
      resultColumnNames[6] = "TYPE";
      resultColumnNames[7] = "ORDINAL_POSITION";
      resultColumnNames[8] = "COLUMN_NAME";
      resultColumnNames[9] = "ASC_OR_DESC";
      resultColumnNames[10] = "PK";
      resultColumnNames[11] = "FullTextSearch";
      resultColumnNames[12] = "KEY_EXPRESSION";
    }

    private void FillRow(IRow row, IVistaDBIndexInformation indexInfo, int keyColumnIndex)
    {
            row[0].Value = Path.GetFileNameWithoutExtension(parent.Database.Name);
            row[1].Value = 91.ToString() + "DBO" + ']';
            row[2].Value = tableName;
            row[3].Value = (short)(indexInfo.Unique ? 0 : 1);
            row[4].Value = tableName;
            row[5].Value = indexInfo.Name;
            row[6].Value = (short)3;
            row[7].Value = keyColumnIndex + 1;
      int rowIndex = indexInfo.KeyStructure[keyColumnIndex].RowIndex;
      bool descending = indexInfo.KeyStructure[keyColumnIndex].Descending;
      this.keyColumnIndex = ++keyColumnIndex < indexInfo.KeyStructure.Length ? keyColumnIndex : -1;
            row[8].Value = schema[rowIndex].Name;
            row[9].Value = descending ? "D" : (object) "A";
            row[10].Value = indexInfo.Primary ? 1 : 0;
            row[11].Value = indexInfo.FullTextSearch ? 1 : 0;
            row[12].Value = indexInfo.KeyExpression;
    }

    protected override object ExecuteSubProgram()
    {
      tableName = paramValues[0].Value as string;
      try
      {
        schema = parent.Database.TableSchema(tableName);
      }
      catch (VistaDBException ex)
      {
        throw new VistaDBSQLException(ex, 572, tableName, lineNo, symbolNo);
      }
      catch
      {
        throw;
      }
      enumerator = schema.Indexes.GetEnumerator();
      return null;
    }

    public override bool First(IRow row)
    {
      enumerator.Reset();
      if (!enumerator.MoveNext())
        return false;
      IVistaDBIndexInformation current = enumerator.Current as IVistaDBIndexInformation;
      FillRow(row, current, 0);
      return true;
    }

    public override bool GetNextResult(IRow row)
    {
      if (keyColumnIndex < 0)
      {
        if (!enumerator.MoveNext())
          return false;
        keyColumnIndex = 0;
      }
      FillRow(row, enumerator.Current as IVistaDBIndexInformation, keyColumnIndex);
      return true;
    }

    public override void Close()
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }
  }
}
