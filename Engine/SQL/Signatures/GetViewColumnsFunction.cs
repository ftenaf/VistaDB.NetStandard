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
      if (ParamCount > 1)
        throw new VistaDBSQLException(501, "GETVIEWCOLUMNS", lineNo, symbolNo);
      if (ParamCount == 1)
        parameterTypes[0] = VistaDBType.NChar;
      resultColumnTypes[0] = VistaDBType.NVarChar;
      resultColumnTypes[1] = VistaDBType.NVarChar;
      resultColumnTypes[2] = VistaDBType.NVarChar;
      resultColumnTypes[3] = VistaDBType.Int;
      resultColumnTypes[4] = VistaDBType.TinyInt;
      resultColumnTypes[5] = VistaDBType.NVarChar;
      resultColumnTypes[6] = VistaDBType.Bit;
      resultColumnTypes[7] = VistaDBType.Int;
      resultColumnTypes[8] = VistaDBType.Int;
      resultColumnTypes[9] = VistaDBType.Bit;
      resultColumnTypes[10] = VistaDBType.NVarChar;
      resultColumnTypes[11] = VistaDBType.NVarChar;
      resultColumnTypes[12] = VistaDBType.Bit;
      resultColumnTypes[13] = VistaDBType.Bit;
      resultColumnTypes[14] = VistaDBType.Bit;
      resultColumnTypes[15] = VistaDBType.NVarChar;
      resultColumnTypes[16] = VistaDBType.NVarChar;
      resultColumnTypes[17] = VistaDBType.NVarChar;
      resultColumnTypes[18] = VistaDBType.NVarChar;
      resultColumnTypes[19] = VistaDBType.Bit;
      resultColumnNames[0] = "VIEW_NAME";
      resultColumnNames[1] = "COLUMN_NAME";
      resultColumnNames[2] = "BASE_COLUMN_NAME";
      resultColumnNames[3] = "COLUMN_ORDINAL";
      resultColumnNames[4] = "DATA_TYPE";
      resultColumnNames[5] = "DATA_TYPE_NAME";
      resultColumnNames[6] = "IS_EXTENDED_TYPE";
      resultColumnNames[7] = "COLUMN_SIZE";
      resultColumnNames[8] = "CODE_PAGE";
      resultColumnNames[9] = "ENCRYPTED";
      resultColumnNames[10] = "COLUMN_CAPTION";
      resultColumnNames[11] = "COLUMN_DESCRIPTION";
      resultColumnNames[12] = "IS_UNIQUE";
      resultColumnNames[13] = "IS_KEY";
      resultColumnNames[14] = "ALLOW_NULL";
      resultColumnNames[15] = "IDENTITY_VALUE";
      resultColumnNames[16] = "IDENTITY_STEP";
      resultColumnNames[17] = "IDENTITY_SEED";
      resultColumnNames[18] = "DEFAULT_VALUE";
      resultColumnNames[19] = "USE_DEFVAL_IN_UPDATE";
      enumerator = null;
      schema = null;
      columnIndex = -1;
      viewName = null;
      columnNames = null;
      searchedView = null;
    }

    protected override object ExecuteSubProgram()
    {
      schema = null;
      columnIndex = -1;
      viewName = null;
      columnNames = null;
      if (ParamCount == 1)
      {
        enumerator = null;
        searchedView = (IView) parent.Database.EnumViews()[paramValues[0].Value];
      }
      else
      {
        enumerator = parent.Database.EnumViews().GetEnumerator();
        searchedView = null;
      }
      return null;
    }

    public override int GetWidth()
    {
      return 0;
    }

    private bool RetrieveSelectCommand()
    {
      schema = null;
      IView view;
      if (enumerator == null)
      {
        if (searchedView == null)
          return false;
        view = searchedView;
      }
      else
        view = (IView) enumerator.Current;
      CreateViewStatement createViewStatement = null;
      try
      {
        Statement statement = (Statement) parent.Connection.CreateBatchStatement(view.Expression, 0L).SubQuery(0);
        createViewStatement = statement as CreateViewStatement;
        if (createViewStatement != null)
        {
          int num = (int) statement.PrepareQuery();
          schema = ((CreateViewStatement)statement).SelectStatement;
          columnIndex = 0;
          viewName = view.Name;
          columnNames = ((CreateViewStatement) statement).ColumnNames;
          createViewStatement.DropTemporaryTables();
        }
      }
      catch (Exception)
            {
      }
      finally
      {
        createViewStatement?.DropTemporaryTables();
      }
      return schema != null;
    }

    private void FillRow(IRow row)
    {
      string aliasName = schema.GetAliasName(columnIndex);
            row[0].Value = viewName;
            row[1].Value = columnNames == null ? aliasName : (object) columnNames[columnIndex];
            row[2].Value = aliasName;
            row[3].Value = columnIndex;
            row[4].Value = (byte)schema.GetColumnVistaDBType(columnIndex);
            row[5].Value = schema.GetColumnVistaDBType(columnIndex).ToString();
            row[6].Value = schema.GetIsLong(columnIndex);
            row[7].Value = schema.GetWidth(columnIndex);
            row[8].Value = schema.GetCodePage(columnIndex);
            row[9].Value = schema.GetIsEncrypted(columnIndex);
            row[10].Value = schema.GetColumnCaption(columnIndex);
            row[11].Value = schema.GetColumnDescription(columnIndex);
            row[12].Value = schema.GetIsKey(columnIndex);
            row[13].Value = row[12].Value;
            row[14].Value = schema.GetIsAllowNull(columnIndex);
      string step;
      string seed;
            row[15].Value = schema.GetIdentity(columnIndex, out step, out seed);
            row[16].Value = step;
            row[17].Value = seed;
      bool useInUpdate;
            row[18].Value = schema.GetDefaultValue(columnIndex, out useInUpdate);
            row[19].Value = useInUpdate;
    }

    public override bool First(IRow row)
    {
      if (enumerator == null)
      {
        if (!RetrieveSelectCommand())
          return false;
      }
      else
      {
        enumerator.Reset();
        while (enumerator.MoveNext())
        {
          if (RetrieveSelectCommand())
            goto label_7;
        }
        return false;
      }
label_7:
      FillRow(row);
      return true;
    }

    public override bool GetNextResult(IRow row)
    {
      ++columnIndex;
      if (columnIndex >= schema.ColumnCount)
      {
        if (enumerator == null)
          return false;
        while (enumerator.MoveNext())
        {
          if (RetrieveSelectCommand())
            goto label_6;
        }
        return false;
      }
label_6:
      FillRow(row);
      return true;
    }

    public override void Close()
    {
      enumerator = null;
      schema = null;
      columnIndex = -1;
      viewName = null;
      columnNames = null;
      searchedView = null;
    }
  }
}
