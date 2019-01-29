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
      this.keyColumnIndex = -1;
      if (this.ParamCount > 1)
        throw new VistaDBSQLException(501, "SP_INDEXES", this.lineNo, this.symbolNo);
      if (this.ParamCount == 1)
        this.parameterTypes[0] = VistaDBType.NChar;
      this.resultColumnTypes[0] = VistaDBType.NVarChar;
      this.resultColumnTypes[1] = VistaDBType.NVarChar;
      this.resultColumnTypes[2] = VistaDBType.NVarChar;
      this.resultColumnTypes[3] = VistaDBType.SmallInt;
      this.resultColumnTypes[4] = VistaDBType.NVarChar;
      this.resultColumnTypes[5] = VistaDBType.NVarChar;
      this.resultColumnTypes[6] = VistaDBType.SmallInt;
      this.resultColumnTypes[7] = VistaDBType.Int;
      this.resultColumnTypes[8] = VistaDBType.NVarChar;
      this.resultColumnTypes[9] = VistaDBType.VarChar;
      this.resultColumnTypes[10] = VistaDBType.Int;
      this.resultColumnTypes[11] = VistaDBType.Int;
      this.resultColumnTypes[12] = VistaDBType.NVarChar;
      this.resultColumnNames[0] = "TABLE_CAT";
      this.resultColumnNames[1] = "TABLE_SCHEM";
      this.resultColumnNames[2] = "TABLE_NAME";
      this.resultColumnNames[3] = "NON_UNIQUE";
      this.resultColumnNames[4] = "INDEX_QUALIFER";
      this.resultColumnNames[5] = "INDEX_NAME";
      this.resultColumnNames[6] = "TYPE";
      this.resultColumnNames[7] = "ORDINAL_POSITION";
      this.resultColumnNames[8] = "COLUMN_NAME";
      this.resultColumnNames[9] = "ASC_OR_DESC";
      this.resultColumnNames[10] = "PK";
      this.resultColumnNames[11] = "FullTextSearch";
      this.resultColumnNames[12] = "KEY_EXPRESSION";
    }

    private void FillRow(IRow row, IVistaDBIndexInformation indexInfo, int keyColumnIndex)
    {
      ((IValue) row[0]).Value = (object) Path.GetFileNameWithoutExtension(this.parent.Database.Name);
      ((IValue) row[1]).Value = (object) (91.ToString() + "DBO" + (object) ']');
      ((IValue) row[2]).Value = (object) this.tableName;
      ((IValue) row[3]).Value = (object) (short) (indexInfo.Unique ? 0 : 1);
      ((IValue) row[4]).Value = (object) this.tableName;
      ((IValue) row[5]).Value = (object) indexInfo.Name;
      ((IValue) row[6]).Value = (object) (short) 3;
      ((IValue) row[7]).Value = (object) (keyColumnIndex + 1);
      int rowIndex = indexInfo.KeyStructure[keyColumnIndex].RowIndex;
      bool descending = indexInfo.KeyStructure[keyColumnIndex].Descending;
      this.keyColumnIndex = ++keyColumnIndex < indexInfo.KeyStructure.Length ? keyColumnIndex : -1;
      ((IValue) row[8]).Value = (object) this.schema[rowIndex].Name;
      ((IValue) row[9]).Value = descending ? (object) "D" : (object) "A";
      ((IValue) row[10]).Value = (object) (indexInfo.Primary ? 1 : 0);
      ((IValue) row[11]).Value = (object) (indexInfo.FullTextSearch ? 1 : 0);
      ((IValue) row[12]).Value = (object) indexInfo.KeyExpression;
    }

    protected override object ExecuteSubProgram()
    {
      this.tableName = ((IValue) this.paramValues[0]).Value as string;
      try
      {
        this.schema = this.parent.Database.TableSchema(this.tableName);
      }
      catch (VistaDBException ex)
      {
        throw new VistaDBSQLException((Exception) ex, 572, this.tableName, this.lineNo, this.symbolNo);
      }
      catch
      {
        throw;
      }
      this.enumerator = (IEnumerator) this.schema.Indexes.GetEnumerator();
      return (object) null;
    }

    public override bool First(IRow row)
    {
      this.enumerator.Reset();
      if (!this.enumerator.MoveNext())
        return false;
      IVistaDBIndexInformation current = this.enumerator.Current as IVistaDBIndexInformation;
      this.FillRow(row, current, 0);
      return true;
    }

    public override bool GetNextResult(IRow row)
    {
      if (this.keyColumnIndex < 0)
      {
        if (!this.enumerator.MoveNext())
          return false;
        this.keyColumnIndex = 0;
      }
      this.FillRow(row, this.enumerator.Current as IVistaDBIndexInformation, this.keyColumnIndex);
      return true;
    }

    public override void Close()
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }
  }
}
