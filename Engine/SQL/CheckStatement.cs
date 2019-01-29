using VistaDB.Diagnostic;
using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal class CheckStatement : Statement
  {
    private Signature signature;
    private RowSourceTable table;

    public CheckStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id, string tableName, Row row)
      : base(connection, parent, parser, id)
    {
      this.table = new RowSourceTable((Statement) this, tableName, row);
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      this.signature = parser.NextSignature(true, true, 6);
      if (parser.SkipToken(false))
        throw new VistaDBSQLException(618, "", parser.TokenValue.RowNo, parser.TokenValue.ColNo);
    }

    public override SourceTable GetTableByAlias(string tableAlias)
    {
      if (this.connection.CompareString(this.table.Alias, tableAlias, true) != 0)
        return (SourceTable) null;
      return (SourceTable) this.table;
    }

    public override SearchColumnResult GetTableByColumnName(string columnName, out SourceTable table, out int columnIndex)
    {
      table = (SourceTable) this.table;
      columnIndex = this.table.Schema.GetColumnOrdinal(columnName);
      return columnIndex < 0 ? SearchColumnResult.NotFound : SearchColumnResult.Found;
    }

    public override SourceTable GetSourceTable(int index)
    {
      return (SourceTable) this.table;
    }

    protected override VistaDBType OnPrepareQuery()
    {
      this.table.Prepare();
      if (this.signature.Prepare() == SignatureType.Constant && this.signature.SignatureType != SignatureType.Constant)
        this.signature = (Signature) ConstantSignature.CreateSignature(this.signature.Execute(), (Statement) this);
      if (this.signature.DataType != VistaDBType.Bit)
        throw new VistaDBSQLException(564, "", this.signature.LineNo, this.signature.SymbolNo);
      this.table.Unprepare();
      return VistaDBType.Bit;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      return (IQueryResult) null;
    }

    public bool Evaluate(Row row)
    {
      this.table.SetRow(row);
      IColumn column = this.signature.Execute();
      if (!this.signature.IsNull)
        return (bool) ((IValue) column).Value;
      return true;
    }
  }
}
