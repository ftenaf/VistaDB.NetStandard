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
      name = parser.GetTableName((Statement) this);
      parser.SkipToken(true);
      if (parser.IsToken("DESCRIPTION"))
      {
        parser.SkipToken(true);
        description = parser.TokenValue.Token;
        parser.SkipToken(true);
      }
      else
        description = (string) null;
      ParseColumnNames(parser);
      parser.ExpectedExpression("AS");
      parser.SkipToken(true);
      parser.ExpectedExpression("SELECT");
      Statement parent = parser.Parent;
      int symbolNo = parser.TokenValue.SymbolNo;
      statement = new SelectStatement(this.connection, (Statement) null, parser, 0L);
      parser.Parent = parent;
      int length = (parser.TokenValue.SymbolNo == 0 ? parser.Text.Length : parser.TokenValue.SymbolNo) - symbolNo;
      statement.CommandText = parser.Text.Substring(symbolNo, length).TrimStart();
    }

    private void ParseColumnNames(SQLParser parser)
    {
      if (!parser.IsToken("("))
      {
        columnNames = (List<string>) null;
      }
      else
      {
        SQLParser.TokenValueClass tokenValue = parser.TokenValue;
        columnNames = new List<string>();
        do
        {
          parser.SkipToken(true);
          columnNames.Add(tokenValue.Token);
          parser.SkipToken(true);
        }
        while (parser.IsToken(","));
        parser.ExpectedExpression(")");
        parser.SkipToken(true);
      }
    }

    protected override VistaDBType OnPrepareQuery()
    {
      if (connection.GetCheckView())
      {
        int num = (int) statement.PrepareQuery();
        if (columnNames != null && statement.ColumnCount != columnNames.Count)
          throw new VistaDBSQLException(602, "", lineNo, symbolNo);
      }
      return VistaDBType.Unknown;
    }

    internal void DropTemporaryTables()
    {
      int index = 0;
      for (int sourceTableCount = statement.SourceTableCount; index < sourceTableCount; ++index)
        (statement.GetSourceTable(index) as FuncSourceTable)?.Function.Close();
    }

    protected override IQueryResult OnExecuteQuery()
    {
      try
      {
        CheckView(Database.EnumViews(), name);
        string str = !replaceAlter ? commandText : "CREATE" + commandText.Substring("ALTER".Length);
        IView viewInstance = Database.CreateViewInstance(name);
        viewInstance.Expression = str;
        viewInstance.Description = description;
        Database.CreateViewObject(viewInstance);
        return (IQueryResult) null;
      }
      finally
      {
        DropTemporaryTables();
      }
    }

    protected virtual void CheckView(IViewList views, string name)
    {
      if (views.Contains((object) name))
        throw new VistaDBSQLException(603, name, lineNo, symbolNo);
    }

    public List<string> ColumnNames
    {
      get
      {
        return columnNames;
      }
    }

    public string Description
    {
      get
      {
        return description;
      }
    }

    public SelectStatement SelectStatement
    {
      get
      {
        return statement;
      }
    }
  }
}
