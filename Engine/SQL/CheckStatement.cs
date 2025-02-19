﻿using VistaDB.Diagnostic;
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
      table = new RowSourceTable(this, tableName, row);
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      signature = parser.NextSignature(true, true, 6);
      if (parser.SkipToken(false))
        throw new VistaDBSQLException(618, "", parser.TokenValue.RowNo, parser.TokenValue.ColNo);
    }

    public override SourceTable GetTableByAlias(string tableAlias)
    {
      if (connection.CompareString(table.Alias, tableAlias, true) != 0)
        return null;
      return table;
    }

    public override SearchColumnResult GetTableByColumnName(string columnName, out SourceTable table, out int columnIndex)
    {
      table = this.table;
      columnIndex = this.table.Schema.GetColumnOrdinal(columnName);
      return columnIndex < 0 ? SearchColumnResult.NotFound : SearchColumnResult.Found;
    }

    public override SourceTable GetSourceTable(int index)
    {
      return table;
    }

    protected override VistaDBType OnPrepareQuery()
    {
      table.Prepare();
      if (signature.Prepare() == SignatureType.Constant && signature.SignatureType != SignatureType.Constant)
        signature = ConstantSignature.CreateSignature(signature.Execute(), this);
      if (signature.DataType != VistaDBType.Bit)
        throw new VistaDBSQLException(564, "", signature.LineNo, signature.SymbolNo);
      table.Unprepare();
      return VistaDBType.Bit;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      return null;
    }

    public bool Evaluate(Row row)
    {
      table.SetRow(row);
      IColumn column = signature.Execute();
      if (!signature.IsNull)
        return (bool)column.Value;
      return true;
    }
  }
}
