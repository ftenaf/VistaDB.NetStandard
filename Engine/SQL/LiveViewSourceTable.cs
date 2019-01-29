using System.Collections.Generic;
using VistaDB.Engine.Core;
using VistaDB.Engine.Core.Cryptography;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class LiveViewSourceTable : BaseViewSourceTable
  {
    private VistaDB.Engine.SQL.Signatures.Signature[] signatures;
    private Row patternRow;
    private bool opened;
    private bool eof;

    public LiveViewSourceTable(Statement parent, IView view, List<string> columnNames, SelectStatement statement, string alias, int index, int lineNo, int symbolNo)
      : base(parent, view, columnNames, statement, alias, index, lineNo, symbolNo)
    {
      this.signatures = (VistaDB.Engine.SQL.Signatures.Signature[]) null;
      this.patternRow = (Row) null;
      this.opened = false;
      this.eof = false;
    }

    private void PrepareFirstOpen()
    {
      IDatabase database = this.statement.Database;
      IQuerySchemaInfo statement = (IQuerySchemaInfo) this.statement;
      this.signatures = new VistaDB.Engine.SQL.Signatures.Signature[statement.ColumnCount];
      this.patternRow = Row.CreateInstance(0U, true, (Encryption) null, (int[]) null);
      int ordinal = 0;
      for (int columnCount = statement.ColumnCount; ordinal < columnCount; ++ordinal)
        this.patternRow.AppendColumn(database.CreateEmptyColumn(statement.GetColumnVistaDBType(ordinal)));
    }

    public override IColumn SimpleGetColumn(int colIndex)
    {
      return this.signatures[colIndex].Execute();
    }

    public override void Close()
    {
      if (!this.opened)
        return;
      this.opened = false;
      this.updateTable = (SourceTable) null;
      this.statement.Close();
    }

    public override void FreeTable()
    {
      this.opened = false;
      this.updateTable = (SourceTable) null;
      this.statement.FreeTables();
    }

    public override int GetColumnCount()
    {
      return this.signatures.Length;
    }

    protected override void OnOpen(bool readOnly)
    {
      if (this.signatures == null)
        this.PrepareFirstOpen();
      this.statement.ExecuteLiveQuery(this.signatures, readOnly, out this.updateTable);
      this.eof = this.statement.EndOfTable;
      this.opened = true;
    }

    protected override bool OnFirst()
    {
      if (!this.opened)
        return false;
      this.statement.FirstRow();
      this.eof = this.statement.EndOfTable;
      return !this.eof;
    }

    protected override bool OnNext()
    {
      this.statement.NextRow();
      this.eof = this.statement.EndOfTable;
      return !this.eof;
    }

    protected override void InternalDeleteRow()
    {
      base.InternalDeleteRow();
      this.eof = this.updateTable.Eof;
    }

    public override bool Eof
    {
      get
      {
        return this.eof;
      }
    }

    public override bool Opened
    {
      get
      {
        return this.opened;
      }
    }
  }
}
