using System;
using System.Collections.Generic;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal class InsertStatement : Statement
  {
    private List<InsertStatement.Column> columns = new List<InsertStatement.Column>();
    private SelectStatement select;
    private SourceTable table;
    private List<Signature> values;
    private string tableName;
    private int tableLineNo;
    private int tableSymbolNo;

    public InsertStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      bool flag = false;
      SQLParser.TokenValueClass tokenValue = parser.TokenValue;
      parser.SkipToken(true);
      if (parser.IsToken("INTO"))
        parser.SkipToken(true);
      if (tokenValue.TokenType != TokenType.Unknown && tokenValue.TokenType != TokenType.Name && tokenValue.TokenType != TokenType.ComplexName)
        throw new VistaDBSQLException(585, tokenValue.Token, tokenValue.RowNo, tokenValue.ColNo);
      this.tableName = parser.GetTableName((Statement) this);
      this.tableLineNo = tokenValue.RowNo;
      this.tableSymbolNo = tokenValue.ColNo;
      parser.SkipToken(true);
      if (parser.IsToken("DEFAULT"))
      {
        parser.SkipToken(true);
        parser.ExpectedExpression("VALUES");
      }
      else
      {
        if (parser.IsToken("("))
        {
          parser.SkipToken(true);
          if (tokenValue.TokenType != TokenType.Unknown || !parser.IsToken("SELECT"))
            this.ParseColumns(parser);
          else
            flag = true;
        }
        if (flag)
        {
          this.select = new SelectStatement(connection, (Statement) this, parser, 0L);
          parser.Parent = (Statement) this;
          parser.ExpectedExpression(")");
        }
        else
          this.ParseValues(parser);
      }
      parser.SkipToken(false);
    }

    private void ParseColumns(SQLParser parser)
    {
      SQLParser.TokenValueClass tokenValue = parser.TokenValue;
      while (tokenValue.TokenType == TokenType.Unknown || tokenValue.TokenType == TokenType.Name || tokenValue.TokenType == TokenType.ComplexName)
      {
        this.columns.Add(new InsertStatement.Column(this, tokenValue.Token, tokenValue.RowNo, tokenValue.ColNo));
        parser.SkipToken(true);
        if (parser.IsToken(","))
        {
          parser.SkipToken(true);
        }
        else
        {
          parser.ExpectedExpression(")");
          parser.SkipToken(true);
          return;
        }
      }
      throw new VistaDBSQLException(586, tokenValue.Token, tokenValue.RowNo, tokenValue.ColNo);
    }

    private void ParseValues(SQLParser parser)
    {
      SQLParser.TokenValueClass tokenValue = parser.TokenValue;
      if (parser.IsToken("("))
      {
        parser.SkipToken(true);
        this.select = new SelectStatement(this.connection, (Statement) this, parser, 0L);
        parser.Parent = (Statement) this;
        parser.ExpectedExpression(")");
      }
      else if (tokenValue.TokenType == TokenType.Unknown && parser.IsToken("SELECT"))
      {
        this.select = new SelectStatement(this.connection, (Statement) this, parser, 0L);
        parser.Parent = (Statement) this;
      }
      else
      {
        if (!parser.IsToken("VALUES"))
          throw new VistaDBSQLException(576, "VALUES", tokenValue.RowNo, tokenValue.ColNo);
        parser.SkipToken(true);
        parser.ExpectedExpression("(");
        this.values = new List<Signature>();
        do
        {
          this.values.Add(parser.NextSignature(true, true, 6));
        }
        while (parser.IsToken(","));
        parser.ExpectedExpression(")");
      }
    }

    protected override VistaDBType OnPrepareQuery()
    {
      int tableIndex = 0;
      this.table = (SourceTable) new NativeSourceTable((Statement) this, this.tableName, this.tableName, 0, this.tableLineNo, this.tableSymbolNo);
      this.table = (SourceTable) this.table.PrepareTables((IVistaDBTableNameCollection) null, (IViewList) null, (TableCollection) null, false, ref tableIndex);
      this.table.ReadOnly = false;
      this.table.Prepare();
      this.PrepareColumns();
      this.PrepareValues();
      this.table.Unprepare();
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      this.affectedRows = 0L;
      if (!this.table.Opened)
        this.table.Open();
      this.table.DoOpenExternalRelationships(true, false);
      try
      {
        try
        {
          this.table.PrepareTriggers(TriggerAction.AfterInsert);
          bool justReset = true;
          try
          {
            if (this.values != null)
              this.ExecuteValues();
            else if (this.select != null)
              this.ExecuteSelect();
            else
              this.ExecuteDefaultValues();
            justReset = false;
          }
          finally
          {
            this.table.ExecuteTriggers(TriggerAction.AfterInsert, justReset);
          }
        }
        finally
        {
          this.table.DoFreeExternalRelationships();
          this.Connection.CachedAffectedRows = this.affectedRows;
        }
      }
      catch
      {
        this.table.Close();
        throw;
      }
      this.table.FreeTable();
      return (IQueryResult) null;
    }

    private void PrepareColumns()
    {
      IQuerySchemaInfo schema = this.table.Schema;
      if (this.columns.Count != 0)
      {
        foreach (InsertStatement.Column column in this.columns)
          column.Prepare(schema);
      }
      else
      {
        int num = 0;
        for (int columnCount = schema.ColumnCount; num < columnCount; ++num)
          this.columns.Add(new InsertStatement.Column(this, num, schema.GetColumnVistaDBType(num)));
      }
    }

    private void PrepareValues()
    {
      if (this.values == null && this.select == null)
        return;
      if (this.values != null)
      {
        if (this.columns.Count < this.values.Count)
          throw new VistaDBSQLException(587, "", this.lineNo, this.symbolNo);
        if (this.columns.Count > this.values.Count)
          throw new VistaDBSQLException(588, "", this.lineNo, this.symbolNo);
        for (int index = 0; index < this.values.Count; ++index)
        {
          Signature signature = this.values[index];
          if (signature.Prepare() == SignatureType.Constant && signature.SignatureType != SignatureType.Constant)
            this.values[index] = (Signature) ConstantSignature.CreateSignature(signature.Execute(), this.columns[index].DataType, (Statement) this);
        }
      }
      else
      {
        int num = (int) this.select.PrepareQuery();
        int columnCount = this.select.ColumnCount;
        if (this.columns.Count < columnCount)
          throw new VistaDBSQLException(589, "", this.lineNo, this.symbolNo);
        if (this.columns.Count > columnCount)
          throw new VistaDBSQLException(590, "", this.lineNo, this.symbolNo);
      }
    }

    private void ExecuteDefaultValues()
    {
      this.table.Insert();
      this.table.Post();
      this.affectedRows = 1L;
    }

    private void ExecuteValues()
    {
      this.table.Insert();
      for (int index = 0; index < this.values.Count; ++index)
      {
        Signature signature = this.values[index];
        IColumn columnValue = signature.Execute();
        this.table.PutValue(this.columns[index].ColumnIndex, columnValue);
        signature.SetChanged();
      }
      this.table.Post();
      this.affectedRows = 1L;
    }

    private void ExecuteSelect()
    {
      IQueryResult queryResult = this.select.SourceTableCount != 1 || this.select.HasWhereClause || !this.select.GetSourceTable(0).TableName.Equals(this.table.TableName, StringComparison.OrdinalIgnoreCase) ? this.select.ExecuteQuery() : this.select.ExecuteNonLiveQuery();
      try
      {
        queryResult.FirstRow();
        while (!queryResult.EndOfTable)
        {
          this.table.Insert();
          for (int index = 0; index < this.columns.Count; ++index)
            this.table.PutValue(this.columns[index].ColumnIndex, queryResult.GetColumn(index));
          this.table.Post();
          queryResult.NextRow();
          ++this.affectedRows;
        }
      }
      finally
      {
        queryResult.Close();
      }
    }

    public override void Dispose()
    {
      base.Dispose();
      if (this.select == null)
        return;
      this.select.Dispose();
    }

    private class Column
    {
      private string columnName;
      private int columnIndex;
      private VistaDBType dataType;
      private int lineNo;
      private int symbolNo;
      private InsertStatement parent;

      public Column(InsertStatement parent, string columnName, int lineNo, int symbolNo)
      {
        this.columnName = columnName;
        this.columnIndex = -1;
        this.dataType = VistaDBType.Unknown;
        this.lineNo = lineNo;
        this.symbolNo = symbolNo;
        this.parent = parent;
      }

      public Column(InsertStatement parent, int columnIndex, VistaDBType dataType)
      {
        this.parent = parent;
        this.columnName = (string) null;
        this.columnIndex = columnIndex;
        this.dataType = dataType;
        this.lineNo = -1;
        this.symbolNo = -1;
      }

      public void Prepare(IQuerySchemaInfo schema)
      {
        this.columnIndex = schema.GetColumnOrdinal(this.columnName);
        if (this.columnIndex < 0)
          throw new VistaDBSQLException(567, this.columnName, this.lineNo, this.symbolNo);
        this.dataType = schema.GetColumnVistaDBType(this.columnIndex);
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
