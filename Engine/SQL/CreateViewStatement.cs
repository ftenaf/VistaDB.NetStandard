using System.Collections.Generic;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class CreateViewStatement : BaseCreateStatement
  {
    protected bool replaceAlter;
    private string name;
    private string description;
    private List<string> columnNames;
    private SelectStatement statement;

    public CreateViewStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      parser.SkipToken(true);
      this.name = parser.GetTableName((Statement) this);
      parser.SkipToken(true);
      if (parser.IsToken("DESCRIPTION"))
      {
        parser.SkipToken(true);
        this.description = parser.TokenValue.Token;
        parser.SkipToken(true);
      }
      else
        this.description = (string) null;
      this.ParseColumnNames(parser);
      parser.ExpectedExpression("AS");
      parser.SkipToken(true);
      parser.ExpectedExpression("SELECT");
      Statement parent = parser.Parent;
      int symbolNo = parser.TokenValue.SymbolNo;
      this.statement = new SelectStatement(this.connection, (Statement) null, parser, 0L);
      parser.Parent = parent;
      int length = (parser.TokenValue.SymbolNo == 0 ? parser.Text.Length : parser.TokenValue.SymbolNo) - symbolNo;
      this.statement.CommandText = parser.Text.Substring(symbolNo, length).TrimStart();
    }

    private void ParseColumnNames(SQLParser parser)
    {
      if (!parser.IsToken("("))
      {
        this.columnNames = (List<string>) null;
      }
      else
      {
        SQLParser.TokenValueClass tokenValue = parser.TokenValue;
        this.columnNames = new List<string>();
        do
        {
          parser.SkipToken(true);
          this.columnNames.Add(tokenValue.Token);
          parser.SkipToken(true);
        }
        while (parser.IsToken(","));
        parser.ExpectedExpression(")");
        parser.SkipToken(true);
      }
    }

    protected override VistaDBType OnPrepareQuery()
    {
      if (this.connection.GetCheckView())
      {
        int num = (int) this.statement.PrepareQuery();
        if (this.columnNames != null && this.statement.ColumnCount != this.columnNames.Count)
          throw new VistaDBSQLException(602, "", this.lineNo, this.symbolNo);
      }
      return VistaDBType.Unknown;
    }

    internal void DropTemporaryTables()
    {
      int index = 0;
      for (int sourceTableCount = this.statement.SourceTableCount; index < sourceTableCount; ++index)
        (this.statement.GetSourceTable(index) as FuncSourceTable)?.Function.Close();
    }

    protected override IQueryResult OnExecuteQuery()
    {
      try
      {
        this.CheckView(this.Database.EnumViews(), this.name);
        string str = !this.replaceAlter ? this.commandText : "CREATE" + this.commandText.Substring("ALTER".Length);
        IView viewInstance = this.Database.CreateViewInstance(this.name);
        viewInstance.Expression = str;
        viewInstance.Description = this.description;
        this.Database.CreateViewObject(viewInstance);
        return (IQueryResult) null;
      }
      finally
      {
        this.DropTemporaryTables();
      }
    }

    protected virtual void CheckView(IViewList views, string name)
    {
      if (views.Contains((object) name))
        throw new VistaDBSQLException(603, name, this.lineNo, this.symbolNo);
    }

    public List<string> ColumnNames
    {
      get
      {
        return this.columnNames;
      }
    }

    public string Description
    {
      get
      {
        return this.description;
      }
    }

    public SelectStatement SelectStatement
    {
      get
      {
        return this.statement;
      }
    }
  }
}
