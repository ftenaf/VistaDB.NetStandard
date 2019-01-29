using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class QuerySourceTable : SourceTable
  {
    protected SelectStatement statement;
    protected IQueryResult queryTable;

    public QuerySourceTable(VistaDB.Engine.SQL.Statement parent, SelectStatement statement, string alias, int index, int lineNo, int symbolNo)
      : base(parent, alias, alias, index, lineNo, symbolNo)
    {
      this.statement = statement;
      this.queryTable = (IQueryResult) null;
    }

    public override IColumn SimpleGetColumn(int colIndex)
    {
      return this.queryTable.GetColumn(colIndex);
    }

    public override void Post()
    {
    }

    public override void Close()
    {
      if (this.queryTable == null)
        return;
      this.queryTable.Close();
      this.queryTable = (IQueryResult) null;
    }

    public override void FreeTable()
    {
      this.statement.FreeTables();
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
      return this.queryTable.GetColumnCount();
    }

    protected override void OnOpen(bool readOnly)
    {
      this.queryTable = this.statement.ExecuteQuery();
    }

    protected override bool OnFirst()
    {
      if (this.queryTable == null)
        return false;
      this.queryTable.FirstRow();
      return !this.queryTable.EndOfTable;
    }

    protected override bool OnNext()
    {
      this.queryTable.NextRow();
      return !this.queryTable.EndOfTable;
    }

    protected override IQuerySchemaInfo InternalPrepare()
    {
      int num = (int) this.statement.PrepareQuery();
      return this.statement.GetSchemaInfo();
    }

    protected override void InternalInsert()
    {
      throw new VistaDBSQLException(604, "", this.lineNo, this.symbolNo);
    }

    protected override void InternalPutValue(int columnIndex, IColumn columnValue)
    {
      throw new VistaDBSQLException(604, "", this.lineNo, this.symbolNo);
    }

    protected override void InternalDeleteRow()
    {
      throw new VistaDBSQLException(604, "", this.lineNo, this.symbolNo);
    }

    public override bool Eof
    {
      get
      {
        return this.queryTable.EndOfTable;
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
        return this.queryTable != null;
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
        return this.statement;
      }
    }
  }
}
