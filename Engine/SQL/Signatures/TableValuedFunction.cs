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
      using (IVistaDBTableSchema vistaDbTableSchema = this.parent.Database.TableSchema(this.resultTableStatement.TableName))
      {
        foreach (IVistaDBColumnAttributes columnAttributes in (IEnumerable<IVistaDBColumnAttributes>) vistaDbTableSchema)
          arrayList.Add((object) columnAttributes.Type);
      }
      return (VistaDBType[]) arrayList.ToArray(typeof (VistaDBType));
    }

    public string[] GetResultColumnNames()
    {
      ArrayList arrayList = new ArrayList();
      using (IVistaDBTableSchema vistaDbTableSchema = this.parent.Database.TableSchema(this.resultTableStatement.TableName))
      {
        foreach (IVistaDBColumnAttributes columnAttributes in (IEnumerable<IVistaDBColumnAttributes>) vistaDbTableSchema)
          arrayList.Add((object) columnAttributes.Name);
      }
      return (string[]) arrayList.ToArray(typeof (string));
    }

    public void Open()
    {
      object resValue;
      this.PrepareExecute(out resValue);
      this.tableInstance = this.parent.Database.OpenTable(this.resultTableStatement.TableName, true, true);
    }

    public bool First(IRow row)
    {
      this.tableInstance.First();
      if (this.tableInstance.EndOfTable || this.tableInstance == null)
        return false;
      IVistaDBRow currentRow = this.tableInstance.CurrentRow;
      int index = 0;
      for (int count = row.Count; index < count; ++index)
        ((IValue) row[index]).Value = currentRow[index].Value;
      return true;
    }

    public bool GetNextResult(IRow row)
    {
      this.tableInstance.Next();
      if (this.tableInstance.EndOfTable)
        return false;
      IVistaDBRow currentRow = this.tableInstance.CurrentRow;
      int index = 0;
      for (int count = row.Count; index < count; ++index)
        ((IValue) row[index]).Value = currentRow[index].Value;
      return true;
    }

    public void Close()
    {
      if (this.tableInstance != null)
      {
        this.tableInstance.Close();
        this.tableInstance = (IVistaDBTable) null;
      }
      this.parent.Database.DropTable(this.resultTableStatement.TableName);
    }
  }
}
