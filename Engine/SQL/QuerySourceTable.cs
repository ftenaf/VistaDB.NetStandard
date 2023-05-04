using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class QuerySourceTable : SourceTable
  {
    protected SelectStatement statement;
    protected IQueryResult queryTable;

    public QuerySourceTable(Statement parent, SelectStatement statement, string alias, int index, int lineNo, int symbolNo)
      : base(parent, alias, alias, index, lineNo, symbolNo)
    {
      this.statement = statement;
      queryTable = (IQueryResult) null;
    }

    public override IColumn SimpleGetColumn(int colIndex)
    {
      return queryTable.GetColumn(colIndex);
    }

    public override void Post()
    {
    }

    public override void Close()
    {
      if (queryTable == null)
        return;
      queryTable.Close();
      queryTable = (IQueryResult) null;
    }

    public override void FreeTable()
    {
      statement.FreeTables();
    }

    public override IVistaDBTableSchema GetTableSchema()
    {
      return (IVistaDBTableSchema) null;
    }

    public override IColumn GetLastIdentity(string columnName)
    {
      return (IColumn) null;
    }

    public override string CreateIndex(string expression, bool instantly)
    {
      return (string) null;
    }

    public override int GetColumnCount()
    {
      return queryTable.GetColumnCount();
    }

    protected override void OnOpen(bool readOnly)
    {
      queryTable = statement.ExecuteQuery();
    }

    protected override bool OnFirst()
    {
      if (queryTable == null)
        return false;
      queryTable.FirstRow();
      return !queryTable.EndOfTable;
    }

    protected override bool OnNext()
    {
      queryTable.NextRow();
      return !queryTable.EndOfTable;
    }

    protected override IQuerySchemaInfo InternalPrepare()
    {
      int num = (int) statement.PrepareQuery();
      return statement.GetSchemaInfo();
    }

    protected override void InternalInsert()
    {
      throw new VistaDBSQLException(604, "", lineNo, symbolNo);
    }

    protected override void InternalPutValue(int columnIndex, IColumn columnValue)
    {
      throw new VistaDBSQLException(604, "", lineNo, symbolNo);
    }

    protected override void InternalDeleteRow()
    {
      throw new VistaDBSQLException(604, "", lineNo, symbolNo);
    }

    public override bool Eof
    {
      get
      {
        return queryTable.EndOfTable;
      }
    }

    public override bool IsNativeTable
    {
      get
      {
        return false;
      }
    }

    public override bool Opened
    {
      get
      {
        return queryTable != null;
      }
    }

    public override bool IsUpdatable
    {
      get
      {
        return false;
      }
    }

    public SelectStatement Statement
    {
      get
      {
        return statement;
      }
    }
  }
}
