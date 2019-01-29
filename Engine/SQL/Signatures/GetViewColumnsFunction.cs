using System;
using System.Collections;
using System.Collections.Generic;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class GetViewColumnsFunction : SpecialFunction
  {
    private IQuerySchemaInfo schema;
    private int columnIndex;
    private string viewName;
    private List<string> columnNames;
    private IView searchedView;

    internal GetViewColumnsFunction(SQLParser parser)
      : base(parser, -1, 20)
    {
      if (this.ParamCount > 1)
        throw new VistaDBSQLException(501, "GETVIEWCOLUMNS", this.lineNo, this.symbolNo);
      if (this.ParamCount == 1)
        this.parameterTypes[0] = VistaDBType.NChar;
      this.resultColumnTypes[0] = VistaDBType.NVarChar;
      this.resultColumnTypes[1] = VistaDBType.NVarChar;
      this.resultColumnTypes[2] = VistaDBType.NVarChar;
      this.resultColumnTypes[3] = VistaDBType.Int;
      this.resultColumnTypes[4] = VistaDBType.TinyInt;
      this.resultColumnTypes[5] = VistaDBType.NVarChar;
      this.resultColumnTypes[6] = VistaDBType.Bit;
      this.resultColumnTypes[7] = VistaDBType.Int;
      this.resultColumnTypes[8] = VistaDBType.Int;
      this.resultColumnTypes[9] = VistaDBType.Bit;
      this.resultColumnTypes[10] = VistaDBType.NVarChar;
      this.resultColumnTypes[11] = VistaDBType.NVarChar;
      this.resultColumnTypes[12] = VistaDBType.Bit;
      this.resultColumnTypes[13] = VistaDBType.Bit;
      this.resultColumnTypes[14] = VistaDBType.Bit;
      this.resultColumnTypes[15] = VistaDBType.NVarChar;
      this.resultColumnTypes[16] = VistaDBType.NVarChar;
      this.resultColumnTypes[17] = VistaDBType.NVarChar;
      this.resultColumnTypes[18] = VistaDBType.NVarChar;
      this.resultColumnTypes[19] = VistaDBType.Bit;
      this.resultColumnNames[0] = "VIEW_NAME";
      this.resultColumnNames[1] = "COLUMN_NAME";
      this.resultColumnNames[2] = "BASE_COLUMN_NAME";
      this.resultColumnNames[3] = "COLUMN_ORDINAL";
      this.resultColumnNames[4] = "DATA_TYPE";
      this.resultColumnNames[5] = "DATA_TYPE_NAME";
      this.resultColumnNames[6] = "IS_EXTENDED_TYPE";
      this.resultColumnNames[7] = "COLUMN_SIZE";
      this.resultColumnNames[8] = "CODE_PAGE";
      this.resultColumnNames[9] = "ENCRYPTED";
      this.resultColumnNames[10] = "COLUMN_CAPTION";
      this.resultColumnNames[11] = "COLUMN_DESCRIPTION";
      this.resultColumnNames[12] = "IS_UNIQUE";
      this.resultColumnNames[13] = "IS_KEY";
      this.resultColumnNames[14] = "ALLOW_NULL";
      this.resultColumnNames[15] = "IDENTITY_VALUE";
      this.resultColumnNames[16] = "IDENTITY_STEP";
      this.resultColumnNames[17] = "IDENTITY_SEED";
      this.resultColumnNames[18] = "DEFAULT_VALUE";
      this.resultColumnNames[19] = "USE_DEFVAL_IN_UPDATE";
      this.enumerator = (IEnumerator) null;
      this.schema = (IQuerySchemaInfo) null;
      this.columnIndex = -1;
      this.viewName = (string) null;
      this.columnNames = (List<string>) null;
      this.searchedView = (IView) null;
    }

    protected override object ExecuteSubProgram()
    {
      this.schema = (IQuerySchemaInfo) null;
      this.columnIndex = -1;
      this.viewName = (string) null;
      this.columnNames = (List<string>) null;
      if (this.ParamCount == 1)
      {
        this.enumerator = (IEnumerator) null;
        this.searchedView = (IView) this.parent.Database.EnumViews()[((IValue) this.paramValues[0]).Value];
      }
      else
      {
        this.enumerator = (IEnumerator) this.parent.Database.EnumViews().GetEnumerator();
        this.searchedView = (IView) null;
      }
      return (object) null;
    }

    public override int GetWidth()
    {
      return 0;
    }

    private bool RetrieveSelectCommand()
    {
      this.schema = (IQuerySchemaInfo) null;
      IView view;
      if (this.enumerator == null)
      {
        if (this.searchedView == null)
          return false;
        view = this.searchedView;
      }
      else
        view = (IView) this.enumerator.Current;
      CreateViewStatement createViewStatement = (CreateViewStatement) null;
      try
      {
        Statement statement = (Statement) this.parent.Connection.CreateBatchStatement(view.Expression, 0L).SubQuery(0);
        createViewStatement = statement as CreateViewStatement;
        if (createViewStatement != null)
        {
          int num = (int) statement.PrepareQuery();
          this.schema = (IQuerySchemaInfo) ((CreateViewStatement) statement).SelectStatement;
          this.columnIndex = 0;
          this.viewName = view.Name;
          this.columnNames = ((CreateViewStatement) statement).ColumnNames;
          createViewStatement.DropTemporaryTables();
        }
      }
      catch (Exception ex)
      {
      }
      finally
      {
        createViewStatement?.DropTemporaryTables();
      }
      return this.schema != null;
    }

    private void FillRow(IRow row)
    {
      string aliasName = this.schema.GetAliasName(this.columnIndex);
      ((IValue) row[0]).Value = (object) this.viewName;
      ((IValue) row[1]).Value = this.columnNames == null ? (object) aliasName : (object) this.columnNames[this.columnIndex];
      ((IValue) row[2]).Value = (object) aliasName;
      ((IValue) row[3]).Value = (object) this.columnIndex;
      ((IValue) row[4]).Value = (object) (byte) this.schema.GetColumnVistaDBType(this.columnIndex);
      ((IValue) row[5]).Value = (object) this.schema.GetColumnVistaDBType(this.columnIndex).ToString();
      ((IValue) row[6]).Value = (object) this.schema.GetIsLong(this.columnIndex);
      ((IValue) row[7]).Value = (object) this.schema.GetWidth(this.columnIndex);
      ((IValue) row[8]).Value = (object) this.schema.GetCodePage(this.columnIndex);
      ((IValue) row[9]).Value = (object) this.schema.GetIsEncrypted(this.columnIndex);
      ((IValue) row[10]).Value = (object) this.schema.GetColumnCaption(this.columnIndex);
      ((IValue) row[11]).Value = (object) this.schema.GetColumnDescription(this.columnIndex);
      ((IValue) row[12]).Value = (object) this.schema.GetIsKey(this.columnIndex);
      ((IValue) row[13]).Value = ((IValue) row[12]).Value;
      ((IValue) row[14]).Value = (object) this.schema.GetIsAllowNull(this.columnIndex);
      string step;
      string seed;
      ((IValue) row[15]).Value = (object) this.schema.GetIdentity(this.columnIndex, out step, out seed);
      ((IValue) row[16]).Value = (object) step;
      ((IValue) row[17]).Value = (object) seed;
      bool useInUpdate;
      ((IValue) row[18]).Value = (object) this.schema.GetDefaultValue(this.columnIndex, out useInUpdate);
      ((IValue) row[19]).Value = (object) useInUpdate;
    }

    public override bool First(IRow row)
    {
      if (this.enumerator == null)
      {
        if (!this.RetrieveSelectCommand())
          return false;
      }
      else
      {
        this.enumerator.Reset();
        while (this.enumerator.MoveNext())
        {
          if (this.RetrieveSelectCommand())
            goto label_7;
        }
        return false;
      }
label_7:
      this.FillRow(row);
      return true;
    }

    public override bool GetNextResult(IRow row)
    {
      ++this.columnIndex;
      if (this.columnIndex >= this.schema.ColumnCount)
      {
        if (this.enumerator == null)
          return false;
        while (this.enumerator.MoveNext())
        {
          if (this.RetrieveSelectCommand())
            goto label_6;
        }
        return false;
      }
label_6:
      this.FillRow(row);
      return true;
    }

    public override void Close()
    {
      this.enumerator = (IEnumerator) null;
      this.schema = (IQuerySchemaInfo) null;
      this.columnIndex = -1;
      this.viewName = (string) null;
      this.columnNames = (List<string>) null;
      this.searchedView = (IView) null;
    }
  }
}
