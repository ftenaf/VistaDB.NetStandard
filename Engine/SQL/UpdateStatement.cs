using System.Collections.Generic;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal class UpdateStatement : BaseUpdateStatement
  {
    private List<UpdateStatement.SetColumn> setColumns = new List<UpdateStatement.SetColumn>();

    public UpdateStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      SQLParser.TokenValueClass tokenValue = parser.TokenValue;
      parser.SkipToken(true);
      if (tokenValue.TokenType != TokenType.Unknown && tokenValue.TokenType != TokenType.Name && tokenValue.TokenType != TokenType.ComplexName)
        throw new VistaDBSQLException(585, tokenValue.Token, this.lineNo, this.symbolNo);
      string tableName = parser.GetTableName((Statement) this);
      this.destinationTable = (SourceTable) new NativeSourceTable((Statement) this, tableName, tableName, 0, tokenValue.RowNo, tokenValue.ColNo);
      parser.SkipToken(true);
      parser.ExpectedExpression("SET");
      this.ParseColumns(parser);
      base.OnParse(connection, parser);
    }

    private void ParseColumns(SQLParser parser)
    {
      SQLParser.TokenValueClass tokenValue = parser.TokenValue;
      do
      {
        parser.SkipToken(true);
        if (tokenValue.TokenType != TokenType.Unknown && tokenValue.TokenType != TokenType.Name && tokenValue.TokenType != TokenType.ComplexName)
          throw new VistaDBSQLException(586, tokenValue.Token, this.lineNo, this.symbolNo);
        int rowNo = tokenValue.RowNo;
        int colNo = tokenValue.ColNo;
        string objectName;
        string complexName = parser.ParseComplexName(out objectName);
        parser.SkipToken(true);
        parser.ExpectedExpression("=");
        Signature exprValue = parser.NextSignature(true, true, 6);
        this.setColumns.Add(new UpdateStatement.SetColumn((Statement) this, complexName, objectName, exprValue, rowNo, colNo));
      }
      while (parser.IsToken(","));
    }

    protected override void ExecuteSimple()
    {
      this.destinationTable.DoOpenExternalRelationships(false, false);
      try
      {
        base.ExecuteSimple();
      }
      finally
      {
        this.destinationTable.DoFreeExternalRelationships();
      }
    }

    protected override void DoPrepareTriggers()
    {
      this.destinationTable.PrepareTriggers(TriggerAction.AfterUpdate);
    }

    protected override void DoExecuteTriggers(bool justReset)
    {
      this.destinationTable.ExecuteTriggers(TriggerAction.AfterUpdate, justReset);
    }

    protected override void PrepareSetColumns()
    {
      IQuerySchemaInfo schema = this.destinationTable.Schema;
      foreach (UpdateStatement.SetColumn setColumn in this.setColumns)
        setColumn.Prepare(schema);
    }

    protected override bool AcceptRow()
    {
      if (!this.destinationTable.RowAvailable)
        return true;
      foreach (UpdateStatement.SetColumn setColumn in this.setColumns)
        this.destinationTable.PutValue(setColumn.ColumnIndex, setColumn.Execute());
      this.destinationTable.Post();
      ++this.affectedRows;
      return true;
    }

    public override void SetChanged()
    {
      this.whereClause.SetChanged();
      foreach (UpdateStatement.SetColumn setColumn in this.setColumns)
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
        this.columnIndex = -1;
        this.lineNo = lineNo;
        this.symbolNo = symbolNo;
        this.dataType = VistaDBType.Unknown;
      }

      public void Prepare(IQuerySchemaInfo schema)
      {
        this.columnIndex = schema.GetColumnOrdinal(this.columnName);
        if (this.columnIndex < 0)
          throw new VistaDBSQLException(567, this.columnName, this.lineNo, this.symbolNo);
        this.dataType = schema.GetColumnVistaDBType(this.columnIndex);
        if (this.exprValue.Prepare() != SignatureType.Constant || this.exprValue.SignatureType == SignatureType.Constant)
          return;
        this.exprValue = (Signature) ConstantSignature.CreateSignature(this.exprValue.Execute(), this.DataType, this.parent);
      }

      public IColumn Execute()
      {
        return this.exprValue.Execute();
      }

      public void SetChanged()
      {
        this.exprValue.SetChanged();
      }

      public int ColumnIndex
      {
        get
        {
          return this.columnIndex;
        }
      }

      public VistaDBType DataType
      {
        get
        {
          return this.dataType;
        }
      }
    }
  }
}
