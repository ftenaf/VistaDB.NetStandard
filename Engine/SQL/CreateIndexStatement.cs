using System.Collections.Generic;
using System.Text;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class CreateIndexStatement : BaseCreateStatement
  {
    private bool fullText;
    private bool unique;
    private bool clustered;
    private string indexName;
    private string tableName;
    private List<Column> columns;
    private string expression;

        public CreateIndexStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      if (parser.IsToken("FULLTEXT"))
      {
        fullText = true;
        parser.SkipToken(true);
      }
      else
        fullText = false;
      if (parser.IsToken("UNIQUE"))
      {
        unique = true;
        parser.SkipToken(true);
      }
      else
        unique = false;
      if (parser.IsToken("CLUSTERED"))
      {
        clustered = true;
        parser.SkipToken(true);
      }
      else
      {
        clustered = false;
        if (parser.IsToken("NONCLUSTERED"))
          parser.SkipToken(true);
      }
      parser.ExpectedExpression("INDEX");
      parser.SkipToken(true);
      if (parser.IsToken("ON") && fullText)
      {
        indexName = "_FullTextIndex";
      }
      else
      {
        indexName = parser.TokenValue.Token;
        parser.SkipToken(true);
      }
      parser.ExpectedExpression("ON");
      parser.SkipToken(true);
      tableName = parser.GetTableName((Statement) this);
      parser.SkipToken(true);
      parser.ExpectedExpression("(");
      ParseColumns(parser);
      parser.ExpectedExpression(")");
      parser.SkipToken(false);
      if (!parser.IsToken("WITH"))
        return;
      parser.SkipToken(true);
      parser.ExpectedExpression("(");
      ParseWithOptions(parser);
      parser.ExpectedExpression(")");
      parser.SkipToken(false);
    }

    private void ParseColumns(SQLParser parser)
    {
      expression = (string) null;
      columns = new List<Column>();
      do
      {
        parser.SkipToken(true);
        string token = parser.TokenValue.Token;
        parser.SkipToken(true);
        bool asc;
        if (parser.IsToken("DESC"))
        {
          asc = false;
          parser.SkipToken(true);
        }
        else
        {
          asc = true;
          if (parser.IsToken("ASC"))
            parser.SkipToken(true);
        }
        columns.Add(new Column(token, asc));
      }
      while (parser.IsToken(","));
    }

        private void ParseWithOptions(SQLParser parser)
    {
      parser.SkipToken(true);
      while (!parser.IsToken(")"))
      {
        if (parser.IsToken("("))
        {
          ParseWithOptions(parser);
          parser.ExpectedExpression(")");
          parser.SkipToken(false);
        }
        else
        {
          if (parser.IsToken(";"))
            throw new VistaDBSQLException(509, "Unrecognized statement separator in the WITH options in CREATE INDEX statement.", lineNo, symbolNo);
          parser.SkipToken(true);
        }
      }
    }

    protected override IQueryResult OnExecuteQuery()
    {
      int num = clustered ? 1 : 0;
      base.OnExecuteQuery();
      using (ITable table = (ITable) Database.OpenTable(tableName, false, false))
      {
        if (fullText)
          table.CreateFTSIndex(indexName, GetExpression());
        else
          table.CreateIndex(indexName, GetExpression(), false, unique);
        return (IQueryResult) null;
      }
    }

    private string GetExpression()
    {
      StringBuilder stringBuilder = new StringBuilder();
      foreach (Column column in columns)
      {
        if (stringBuilder.Length > 0)
          stringBuilder.Append(";");
        if (column.Ascending)
          stringBuilder.Append(column.Name);
        else
          stringBuilder.Append("DESC(" + column.Name + ")");
      }
      return stringBuilder.ToString();
    }

    private class Column
    {
      private readonly string name;
      private readonly bool asc;

      public Column(string name, bool asc)
      {
        this.name = name;
        this.asc = asc;
      }

      public string Name
      {
        get
        {
          return name;
        }
      }

      public bool Ascending
      {
        get
        {
          return asc;
        }
      }
    }
  }
}
