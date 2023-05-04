using System;
using System.Collections;
using System.Collections.Generic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class GetViewsFunction : SpecialFunction
  {
    internal GetViewsFunction(SQLParser parser)
      : base(parser, 0, 7)
    {
      resultColumnTypes[0] = VistaDBType.NVarChar;
      resultColumnTypes[1] = VistaDBType.Text;
      resultColumnTypes[2] = VistaDBType.Text;
      resultColumnTypes[3] = VistaDBType.Text;
      resultColumnTypes[4] = VistaDBType.NVarChar;
      resultColumnTypes[5] = VistaDBType.Bit;
      resultColumnTypes[6] = VistaDBType.Bit;
      resultColumnNames[0] = "VIEW_NAME";
      resultColumnNames[1] = "VIEW_DEFINITION";
      resultColumnNames[2] = "SELECT_COMMAND";
      resultColumnNames[3] = "DESCRIPTION";
      resultColumnNames[4] = "COLUMN_NAMES";
      resultColumnNames[5] = "IS_UPDATABLE";
      resultColumnNames[6] = "IS_CORRECT";
      enumerator = null;
    }

    protected override object ExecuteSubProgram()
    {
      enumerator = parent.Database.EnumViews().GetEnumerator();
      return null;
    }

    public override int GetWidth()
    {
      return 0;
    }

    private void FillRow(IRow row)
    {
      CreateViewStatement createViewStatement = null;
      string str1 = null;
      string str2 = null;
      string empty = string.Empty;
      IView current = (IView) enumerator.Current;
      Statement statement;
      bool flag;
      try
      {
        statement = (Statement) parent.Connection.CreateBatchStatement(current.Expression, 0L).SubQuery(0);
        createViewStatement = statement as CreateViewStatement;
        flag = createViewStatement != null;
        if (flag)
        {
          str1 = ((CreateViewStatement) statement).Description;
          str2 = ((CreateViewStatement) statement).SelectStatement.CommandText;
          List<string> columnNames = ((CreateViewStatement) statement).ColumnNames;
          if (columnNames != null)
          {
            int index = 0;
            for (int count = columnNames.Count; index < count; ++index)
            {
              if (index > 0)
                empty += ", ";
              empty += columnNames[index];
            }
          }
          int num = (int) statement.PrepareQuery();
        }
      }
      catch (Exception)
            {
        statement = null;
        flag = false;
      }
      finally
      {
        createViewStatement?.DropTemporaryTables();
      }
            row[0].Value = current.Name;
            row[1].Value = current.Expression;
            row[2].Value = str2;
            row[3].Value = str1;
            row[4].Value = empty;
            row[5].Value = !flag ? false : (((CreateViewStatement)statement).SelectStatement.IsLiveQuery() ? true : false);
            row[6].Value = flag;
    }

    public override bool First(IRow row)
    {
      enumerator.Reset();
      if (!enumerator.MoveNext())
        return false;
      FillRow(row);
      return true;
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
      enumerator = null;
    }
  }
}
