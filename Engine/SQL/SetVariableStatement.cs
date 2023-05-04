using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal class SetVariableStatement : Statement
  {
    private VistaDBType dataType = VistaDBType.Unknown;
    private string origName;
    private string name;
    private Signature signature;
    private IColumn tempValue;

    public SetVariableStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      origName = parser.TokenValue.Token;
      parser.CheckVariableName();
      name = origName.Substring(1);
      parser.SkipToken(true);
      parser.ExpectedExpression("=");
      signature = parser.NextSignature(true, true, 6);
    }

    protected override VistaDBType OnPrepareQuery()
    {
      IParameter parameter = DoGetParam(name);
      if (parameter == null)
        throw new VistaDBSQLException(616, origName, lineNo, symbolNo);
      dataType = parameter.DataType;
      int num = (int) signature.Prepare();
      return dataType;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      if (tempValue == null)
        tempValue = Database.CreateEmptyColumn(dataType);
      Database.Conversion.Convert((IValue) signature.Execute(), (IValue) tempValue);
      signature.SetChanged();
      DoSetParam(name, ((IValue) tempValue).Value, dataType, DoGetParam(name).Direction);
      return (IQueryResult) null;
    }
  }
}
