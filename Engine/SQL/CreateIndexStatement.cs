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
    private List<CreateIndexStatement.Column> columns;
    private string expression;
    private string forExpression;

    public CreateIndexStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      if (parser.IsToken("FULLTEXT"))
      {
        this.fullText = true;
        parser.SkipToken(true);
      }
      else
        this.fullText = false;
      if (parser.IsToken("UNIQUE"))
      {
        this.unique = true;
        parser.SkipToken(true);
      }
      else
        this.unique = false;
      if (parser.IsToken("CLUSTERED"))
      {
        this.clustered = true;
        parser.SkipToken(true);
      }
      else
      {
        this.clustered = false;
        if (parser.IsToken("NONCLUSTERED"))
          parser.SkipToken(true);
      }
      parser.ExpectedExpression("INDEX");
      parser.SkipToken(true);
      if (parser.IsToken("ON") && this.fullText)
      {
        this.indexName = "_FullTextIndex";
      }
      else
      {
        this.indexName = parser.TokenValue.Token;
        parser.SkipToken(true);
      }
      parser.ExpectedExpression("ON");
      parser.SkipToken(true);
      this.tableName = parser.GetTableName((Statement) this);
      parser.SkipToken(true);
      parser.ExpectedExpression("(");
      this.ParseColumns(parser);
      parser.ExpectedExpression(")");
      parser.SkipToken(false);
      if (!parser.IsToken("WITH"))
        return;
      parser.SkipToken(true);
      parser.ExpectedExpression("(");
      this.ParseWithOptions(parser);
      parser.ExpectedExpression(")");
      parser.SkipToken(false);
    }

    private void ParseColumns(SQLParser parser)
    {
      this.expression = (string) null;
      this.columns = new List<CreateIndexStatement.Column>();
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
        this.columns.Add(new CreateIndexStatement.Column(token, asc));
      }
      while (parser.IsToken(","));
    }

    private void ParseExpression(SQLParser parser)
    {
      this.columns = (List<CreateIndexStatement.Column>) null;
      parser.SkipToken(true);
      this.expression = parser.TokenValue.Token;
      parser.SkipToken(true);
      if (!parser.IsToken(","))
        return;
      parser.SkipToken(true);
      this.forExpression = parser.TokenValue.Token;
      parser.SkipToken(true);
    }

    private void ParseWithOptions(SQLParser parser)
    {
      parser.SkipToken(true);
      while (!parser.IsToken(")"))
      {
        if (parser.IsToken("("))
        {
          this.ParseWithOptions(parser);
          parser.ExpectedExpression(")");
          parser.SkipToken(false);
        }
        else
        {
          if (parser.IsToken(";"))
            throw new VistaDBSQLException(509, "Unrecognized statement separator in the WITH options in CREATE INDEX statement.", this.lineNo, this.symbolNo);
          parser.SkipToken(true);
        }
      }
    }

    protected override IQueryResult OnExecuteQuery()
    {
      int num = this.clustered ? 1 : 0;
      base.OnExecuteQuery();
      using (ITable table = (ITable) this.Database.OpenTable(this.tableName, false, false))
      {
        if (this.fullText)
          table.CreateFTSIndex(this.indexName, this.GetExpression());
        else
          table.CreateIndex(this.indexName, this.GetExpression(), false, this.unique);
        return (IQueryResult) null;
      }
    }

    private string GetExpression()
    {
      StringBuilder stringBuilder = new StringBuilder();
      foreach (CreateIndexStatement.Column column in this.columns)
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
      private string name;
      private bool asc;

      public Column(string name, bool asc)
      {
        this.name = name;
        this.asc = asc;
      }

      public string Name
      {
        get
        {
          return this.name;
        }
      }

      public bool Ascending
      {
        get
        {
          return this.asc;
        }
      }
    }
  }
}
