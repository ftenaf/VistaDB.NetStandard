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
    private List<Column> columns = new List<Column>();
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
      tableName = parser.GetTableName(this);
      tableLineNo = tokenValue.RowNo;
      tableSymbolNo = tokenValue.ColNo;
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
            ParseColumns(parser);
          else
            flag = true;
        }
        if (flag)
        {
          select = new SelectStatement(connection, this, parser, 0L);
          parser.Parent = this;
          parser.ExpectedExpression(")");
        }
        else
          ParseValues(parser);
      }
      parser.SkipToken(false);
    }

    private void ParseColumns(SQLParser parser)
    {
      SQLParser.TokenValueClass tokenValue = parser.TokenValue;
      while (tokenValue.TokenType == TokenType.Unknown || tokenValue.TokenType == TokenType.Name || tokenValue.TokenType == TokenType.ComplexName)
      {
        columns.Add(new Column(this, tokenValue.Token, tokenValue.RowNo, tokenValue.ColNo));
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
        select = new SelectStatement(connection, this, parser, 0L);
        parser.Parent = this;
        parser.ExpectedExpression(")");
      }
      else if (tokenValue.TokenType == TokenType.Unknown && parser.IsToken("SELECT"))
      {
        select = new SelectStatement(connection, this, parser, 0L);
        parser.Parent = this;
      }
      else
      {
        if (!parser.IsToken("VALUES"))
          throw new VistaDBSQLException(576, "VALUES", tokenValue.RowNo, tokenValue.ColNo);
        parser.SkipToken(true);
        parser.ExpectedExpression("(");
        values = new List<Signature>();
        do
        {
          values.Add(parser.NextSignature(true, true, 6));
        }
        while (parser.IsToken(","));
        parser.ExpectedExpression(")");
      }
    }

    protected override VistaDBType OnPrepareQuery()
    {
      int tableIndex = 0;
      table = new NativeSourceTable(this, tableName, tableName, 0, tableLineNo, tableSymbolNo);
      table = (SourceTable) table.PrepareTables(null, null, null, false, ref tableIndex);
      table.ReadOnly = false;
      table.Prepare();
      PrepareColumns();
      PrepareValues();
      table.Unprepare();
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      affectedRows = 0L;
      if (!table.Opened)
        table.Open();
      table.DoOpenExternalRelationships(true, false);
      try
      {
        try
        {
          table.PrepareTriggers(TriggerAction.AfterInsert);
          bool justReset = true;
          try
          {
            if (values != null)
              ExecuteValues();
            else if (select != null)
              ExecuteSelect();
            else
              ExecuteDefaultValues();
            justReset = false;
          }
          finally
          {
            table.ExecuteTriggers(TriggerAction.AfterInsert, justReset);
          }
        }
        finally
        {
          table.DoFreeExternalRelationships();
          Connection.CachedAffectedRows = affectedRows;
        }
      }
      catch
      {
        table.Close();
        throw;
      }
      table.FreeTable();
      return null;
    }

    private void PrepareColumns()
    {
      IQuerySchemaInfo schema = table.Schema;
      if (columns.Count != 0)
      {
        foreach (Column column in columns)
          column.Prepare(schema);
      }
      else
      {
        int num = 0;
        for (int columnCount = schema.ColumnCount; num < columnCount; ++num)
          columns.Add(new Column(this, num, schema.GetColumnVistaDBType(num)));
      }
    }

    private void PrepareValues()
    {
      if (values == null && select == null)
        return;
      if (values != null)
      {
        if (columns.Count < values.Count)
          throw new VistaDBSQLException(587, "", lineNo, symbolNo);
        if (columns.Count > values.Count)
          throw new VistaDBSQLException(588, "", lineNo, symbolNo);
        for (int index = 0; index < values.Count; ++index)
        {
          Signature signature = values[index];
          if (signature.Prepare() == SignatureType.Constant && signature.SignatureType != SignatureType.Constant)
            values[index] = ConstantSignature.CreateSignature(signature.Execute(), columns[index].DataType, this);
        }
      }
      else
      {
        int num = (int) select.PrepareQuery();
        int columnCount = select.ColumnCount;
        if (columns.Count < columnCount)
          throw new VistaDBSQLException(589, "", lineNo, symbolNo);
        if (columns.Count > columnCount)
          throw new VistaDBSQLException(590, "", lineNo, symbolNo);
      }
    }

    private void ExecuteDefaultValues()
    {
      table.Insert();
      table.Post();
      affectedRows = 1L;
    }

    private void ExecuteValues()
    {
      table.Insert();
      for (int index = 0; index < values.Count; ++index)
      {
        Signature signature = values[index];
        IColumn columnValue = signature.Execute();
        table.PutValue(columns[index].ColumnIndex, columnValue);
        signature.SetChanged();
      }
      table.Post();
      affectedRows = 1L;
    }

    private void ExecuteSelect()
    {
      IQueryResult queryResult = select.SourceTableCount != 1 || select.HasWhereClause || !select.GetSourceTable(0).TableName.Equals(table.TableName, StringComparison.OrdinalIgnoreCase) ? select.ExecuteQuery() : select.ExecuteNonLiveQuery();
      try
      {
        queryResult.FirstRow();
        while (!queryResult.EndOfTable)
        {
          table.Insert();
          for (int index = 0; index < columns.Count; ++index)
            table.PutValue(columns[index].ColumnIndex, queryResult.GetColumn(index));
          table.Post();
          queryResult.NextRow();
          ++affectedRows;
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
      if (select == null)
        return;
      select.Dispose();
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
        columnIndex = -1;
        dataType = VistaDBType.Unknown;
        this.lineNo = lineNo;
        this.symbolNo = symbolNo;
        this.parent = parent;
      }

      public Column(InsertStatement parent, int columnIndex, VistaDBType dataType)
      {
        this.parent = parent;
        columnName = null;
        this.columnIndex = columnIndex;
        this.dataType = dataType;
        lineNo = -1;
        symbolNo = -1;
      }

      public void Prepare(IQuerySchemaInfo schema)
      {
        columnIndex = schema.GetColumnOrdinal(columnName);
        if (columnIndex < 0)
          throw new VistaDBSQLException(567, columnName, lineNo, symbolNo);
        dataType = schema.GetColumnVistaDBType(columnIndex);
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
