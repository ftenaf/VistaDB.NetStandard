using System.Collections.Generic;
using VistaDB.Engine.Core;
using VistaDB.Engine.Core.Cryptography;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class LiveViewSourceTable : BaseViewSourceTable
  {
    private Signatures.Signature[] signatures;
    private Row patternRow;
    private bool opened;
    private bool eof;

    public LiveViewSourceTable(Statement parent, IView view, List<string> columnNames, SelectStatement statement, string alias, int index, int lineNo, int symbolNo)
      : base(parent, view, columnNames, statement, alias, index, lineNo, symbolNo)
    {
      signatures = (Signatures.Signature[]) null;
      patternRow = (Row) null;
      opened = false;
      eof = false;
    }

    private void PrepareFirstOpen()
    {
      IDatabase database = this.statement.Database;
      IQuerySchemaInfo statement = (IQuerySchemaInfo) this.statement;
      signatures = new Signatures.Signature[statement.ColumnCount];
      patternRow = Row.CreateInstance(0U, true, (Encryption) null, (int[]) null);
      int ordinal = 0;
      for (int columnCount = statement.ColumnCount; ordinal < columnCount; ++ordinal)
        patternRow.AppendColumn(database.CreateEmptyColumn(statement.GetColumnVistaDBType(ordinal)));
    }

    public override IColumn SimpleGetColumn(int colIndex)
    {
      return signatures[colIndex].Execute();
    }

    public override void Close()
    {
      if (!opened)
        return;
      opened = false;
      updateTable = (SourceTable) null;
      statement.Close();
    }

    public override void FreeTable()
    {
      opened = false;
      updateTable = (SourceTable) null;
      statement.FreeTables();
    }

    public override int GetColumnCount()
    {
      return signatures.Length;
    }

    protected override void OnOpen(bool readOnly)
    {
      if (signatures == null)
        PrepareFirstOpen();
      statement.ExecuteLiveQuery(signatures, readOnly, out updateTable);
      eof = statement.EndOfTable;
      opened = true;
    }

    protected override bool OnFirst()
    {
      if (!opened)
        return false;
      statement.FirstRow();
      eof = statement.EndOfTable;
      return !eof;
    }

    protected override bool OnNext()
    {
      statement.NextRow();
      eof = statement.EndOfTable;
      return !eof;
    }

    protected override void InternalDeleteRow()
    {
      base.InternalDeleteRow();
      eof = updateTable.Eof;
    }

    public override bool Eof
    {
      get
      {
        return eof;
      }
    }

    public override bool Opened
    {
      get
      {
        return opened;
      }
    }
  }
}
