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
      queryTable = (IQueryResult) null;
    }

    public override IColumn SimpleGetColumn(int colIndex)
    {
      return queryTable.GetColumn(colIndex);
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

    public override bool Eof
    {
      get
      {
        return queryTable.EndOfTable;
      }
    }

    public override bool Opened
    {
      get
      {
        return queryTable != null;
      }
    }
  }
}
