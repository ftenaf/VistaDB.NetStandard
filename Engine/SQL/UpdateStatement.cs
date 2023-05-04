using System.Collections.Generic;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal class UpdateStatement : BaseUpdateStatement
  {
    private List<SetColumn> setColumns = new List<SetColumn>();

    public UpdateStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      SQLParser.TokenValueClass tokenValue = parser.TokenValue;
      parser.SkipToken(true);
      if (tokenValue.TokenType != TokenType.Unknown && tokenValue.TokenType != TokenType.Name && tokenValue.TokenType != TokenType.ComplexName)
        throw new VistaDBSQLException(585, tokenValue.Token, lineNo, symbolNo);
      string tableName = parser.GetTableName(this);
      destinationTable = new NativeSourceTable(this, tableName, tableName, 0, tokenValue.RowNo, tokenValue.ColNo);
      parser.SkipToken(true);
      parser.ExpectedExpression("SET");
      ParseColumns(parser);
      base.OnParse(connection, parser);
    }

    private void ParseColumns(SQLParser parser)
    {
      SQLParser.TokenValueClass tokenValue = parser.TokenValue;
      do
      {
        parser.SkipToken(true);
        if (tokenValue.TokenType != TokenType.Unknown && tokenValue.TokenType != TokenType.Name && tokenValue.TokenType != TokenType.ComplexName)
          throw new VistaDBSQLException(586, tokenValue.Token, lineNo, symbolNo);
        int rowNo = tokenValue.RowNo;
        int colNo = tokenValue.ColNo;
        string objectName;
        string complexName = parser.ParseComplexName(out objectName);
        parser.SkipToken(true);
        parser.ExpectedExpression("=");
        Signature exprValue = parser.NextSignature(true, true, 6);
        setColumns.Add(new SetColumn(this, complexName, objectName, exprValue, rowNo, colNo));
      }
      while (parser.IsToken(","));
    }

    protected override void ExecuteSimple()
    {
      destinationTable.DoOpenExternalRelationships(false, false);
      try
      {
        base.ExecuteSimple();
      }
      finally
      {
        destinationTable.DoFreeExternalRelationships();
      }
    }

    protected override void DoPrepareTriggers()
    {
      destinationTable.PrepareTriggers(TriggerAction.AfterUpdate);
    }

    protected override void DoExecuteTriggers(bool justReset)
    {
      destinationTable.ExecuteTriggers(TriggerAction.AfterUpdate, justReset);
    }

    protected override void PrepareSetColumns()
    {
      IQuerySchemaInfo schema = destinationTable.Schema;
      foreach (SetColumn setColumn in setColumns)
        setColumn.Prepare(schema);
    }

    protected override bool AcceptRow()
    {
      if (!destinationTable.RowAvailable)
        return true;
      foreach (SetColumn setColumn in setColumns)
        destinationTable.PutValue(setColumn.ColumnIndex, setColumn.Execute());
      destinationTable.Post();
      ++affectedRows;
      return true;
    }

    public override void SetChanged()
    {
      whereClause.SetChanged();
      foreach (SetColumn setColumn in setColumns)
        setColumn.SetChanged();
    }

    private class SetColumn
    {
      private string tableAlias;
      private string columnName;
      private int columnIndex;
      private Signature exprValue;
      private int lineNo;
      private int symbolNo;
      private VistaDBType dataType;
      private Statement parent;

      public SetColumn(Statement parent, string tableAlias, string columnName, Signature exprValue, int lineNo, int symbolNo)
      {
        this.parent = parent;
        this.tableAlias = tableAlias;
        this.columnName = columnName;
        this.exprValue = exprValue;
        columnIndex = -1;
        this.lineNo = lineNo;
        this.symbolNo = symbolNo;
        dataType = VistaDBType.Unknown;
      }

      public void Prepare(IQuerySchemaInfo schema)
      {
        columnIndex = schema.GetColumnOrdinal(columnName);
        if (columnIndex < 0)
          throw new VistaDBSQLException(567, columnName, lineNo, symbolNo);
        dataType = schema.GetColumnVistaDBType(columnIndex);
        if (exprValue.Prepare() != SignatureType.Constant || exprValue.SignatureType == SignatureType.Constant)
          return;
        exprValue = ConstantSignature.CreateSignature(exprValue.Execute(), DataType, parent);
      }

      public IColumn Execute()
      {
        return exprValue.Execute();
      }

      public void SetChanged()
      {
        exprValue.SetChanged();
      }

      public int ColumnIndex
      {
        get
        {
          return columnIndex;
        }
      }

      public VistaDBType DataType
      {
        get
        {
          return dataType;
        }
      }
    }
  }
}
