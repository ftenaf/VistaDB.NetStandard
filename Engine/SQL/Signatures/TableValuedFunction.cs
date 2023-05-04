using System.Collections;
using System.Collections.Generic;
using VistaDB.DDA;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class TableValuedFunction : StoredFunction, ITableValuedFunction
  {
    private IVistaDBTable tableInstance;

    internal TableValuedFunction(SQLParser parser, IUserDefinedFunctionInformation udfInstance)
      : base(parser, udfInstance)
    {
    }

    public VistaDBType[] GetResultColumnTypes()
    {
      ArrayList arrayList = new ArrayList();
      using (IVistaDBTableSchema vistaDbTableSchema = parent.Database.TableSchema(resultTableStatement.TableName))
      {
        foreach (IVistaDBColumnAttributes columnAttributes in (IEnumerable<IVistaDBColumnAttributes>) vistaDbTableSchema)
          arrayList.Add((object) columnAttributes.Type);
      }
      return (VistaDBType[]) arrayList.ToArray(typeof (VistaDBType));
    }

    public string[] GetResultColumnNames()
    {
      ArrayList arrayList = new ArrayList();
      using (IVistaDBTableSchema vistaDbTableSchema = parent.Database.TableSchema(resultTableStatement.TableName))
      {
        foreach (IVistaDBColumnAttributes columnAttributes in (IEnumerable<IVistaDBColumnAttributes>) vistaDbTableSchema)
          arrayList.Add((object) columnAttributes.Name);
      }
      return (string[]) arrayList.ToArray(typeof (string));
    }

    public void Open()
    {
      object resValue;
      PrepareExecute(out resValue);
      tableInstance = parent.Database.OpenTable(resultTableStatement.TableName, true, true);
    }

    public bool First(IRow row)
    {
      tableInstance.First();
      if (tableInstance.EndOfTable || tableInstance == null)
        return false;
      IVistaDBRow currentRow = tableInstance.CurrentRow;
      int index = 0;
      for (int count = row.Count; index < count; ++index)
        ((IValue) row[index]).Value = currentRow[index].Value;
      return true;
    }

    public bool GetNextResult(IRow row)
    {
      tableInstance.Next();
      if (tableInstance.EndOfTable)
        return false;
      IVistaDBRow currentRow = tableInstance.CurrentRow;
      int index = 0;
      for (int count = row.Count; index < count; ++index)
        ((IValue) row[index]).Value = currentRow[index].Value;
      return true;
    }

    public void Close()
    {
      if (tableInstance != null)
      {
        tableInstance.Close();
        tableInstance = (IVistaDBTable) null;
      }
      parent.Database.DropTable(resultTableStatement.TableName);
    }
  }
}
