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
      this.resultColumnTypes[0] = VistaDBType.NVarChar;
      this.resultColumnTypes[1] = VistaDBType.Text;
      this.resultColumnTypes[2] = VistaDBType.Text;
      this.resultColumnTypes[3] = VistaDBType.Text;
      this.resultColumnTypes[4] = VistaDBType.NVarChar;
      this.resultColumnTypes[5] = VistaDBType.Bit;
      this.resultColumnTypes[6] = VistaDBType.Bit;
      this.resultColumnNames[0] = "VIEW_NAME";
      this.resultColumnNames[1] = "VIEW_DEFINITION";
      this.resultColumnNames[2] = "SELECT_COMMAND";
      this.resultColumnNames[3] = "DESCRIPTION";
      this.resultColumnNames[4] = "COLUMN_NAMES";
      this.resultColumnNames[5] = "IS_UPDATABLE";
      this.resultColumnNames[6] = "IS_CORRECT";
      this.enumerator = (IEnumerator) null;
    }

    protected override object ExecuteSubProgram()
    {
      this.enumerator = (IEnumerator) this.parent.Database.EnumViews().GetEnumerator();
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
      IView current = (IView) this.enumerator.Current;
      Statement statement;
      bool flag;
      try
      {
        statement = (Statement) this.parent.Connection.CreateBatchStatement(current.Expression, 0L).SubQuery(0);
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
      catch (Exception ex)
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
      this.enumerator.Reset();
      if (!this.enumerator.MoveNext())
        return false;
      this.FillRow(row);
      return true;
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
      this.enumerator = (IEnumerator) null;
    }
  }
}
