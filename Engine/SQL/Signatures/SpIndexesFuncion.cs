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
      ((IValue) row[0]).Value = (object) Path.GetFileNameWithoutExtension(parent.Database.Name);
      ((IValue) row[1]).Value = (object) (91.ToString() + "DBO" + (object) ']');
      ((IValue) row[2]).Value = (object) tableName;
      ((IValue) row[3]).Value = (object) (short) (indexInfo.Unique ? 0 : 1);
      ((IValue) row[4]).Value = (object) tableName;
      ((IValue) row[5]).Value = (object) indexInfo.Name;
      ((IValue) row[6]).Value = (object) (short) 3;
      ((IValue) row[7]).Value = (object) (keyColumnIndex + 1);
      int rowIndex = indexInfo.KeyStructure[keyColumnIndex].RowIndex;
      bool descending = indexInfo.KeyStructure[keyColumnIndex].Descending;
      this.keyColumnIndex = ++keyColumnIndex < indexInfo.KeyStructure.Length ? keyColumnIndex : -1;
      ((IValue) row[8]).Value = (object) schema[rowIndex].Name;
      ((IValue) row[9]).Value = descending ? (object) "D" : (object) "A";
      ((IValue) row[10]).Value = (object) (indexInfo.Primary ? 1 : 0);
      ((IValue) row[11]).Value = (object) (indexInfo.FullTextSearch ? 1 : 0);
      ((IValue) row[12]).Value = (object) indexInfo.KeyExpression;
    }

    protected override object ExecuteSubProgram()
    {
      tableName = ((IValue) paramValues[0]).Value as string;
      try
      {
        schema = parent.Database.TableSchema(tableName);
      }
      catch (VistaDBException ex)
      {
        throw new VistaDBSQLException((Exception) ex, 572, tableName, lineNo, symbolNo);
      }
      catch
      {
        throw;
      }
      enumerator = (IEnumerator) schema.Indexes.GetEnumerator();
      return (object) null;
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
