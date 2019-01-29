using System.Collections.Generic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class QueryViewSourceTable : BaseViewSourceTable
  {
    private IQueryResult queryTable;

    public QueryViewSourceTable(Statement parent, IView view, List<string> columnNames, SelectStatement statement, string alias, int index, int lineNo, int symbolNo)
      : base(parent, view, columnNames, statement, alias, index, lineNo, symbolNo)
    {
      this.queryTable = (IQueryResult) null;
    }

    public override IColumn SimpleGetColumn(int colIndex)
    {
      return this.queryTable.GetColumn(colIndex);
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

    public override bool Eof
    {
      get
      {
        return this.queryTable.EndOfTable;
      }
    }

    public override bool Opened
    {
      get
      {
        return this.queryTable != null;
      }
    }
  }
}
