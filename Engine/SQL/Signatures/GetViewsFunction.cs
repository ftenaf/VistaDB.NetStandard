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
      enumerator = (IEnumerator) null;
    }

    protected override object ExecuteSubProgram()
    {
      enumerator = (IEnumerator) parent.Database.EnumViews().GetEnumerator();
      return (object) null;
    }

    public override int GetWidth()
    {
      return 0;
    }

    private void FillRow(IRow row)
    {
      CreateViewStatement createViewStatement = (CreateViewStatement) null;
      string str1 = (string) null;
      string str2 = (string) null;
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
        statement = (Statement) null;
        flag = false;
      }
      finally
      {
        createViewStatement?.DropTemporaryTables();
      }
      ((IValue) row[0]).Value = (object) current.Name;
      ((IValue) row[1]).Value = (object) current.Expression;
      ((IValue) row[2]).Value = (object) str2;
      ((IValue) row[3]).Value = (object) str1;
      ((IValue) row[4]).Value = (object) empty;
      ((IValue) row[5]).Value = !flag ? false : (((CreateViewStatement)statement).SelectStatement.IsLiveQuery() ? true : false);
      ((IValue) row[6]).Value = (object) flag;
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
      enumerator = (IEnumerator) null;
    }
  }
}
