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
      this.origName = parser.TokenValue.Token;
      parser.CheckVariableName();
      this.name = this.origName.Substring(1);
      parser.SkipToken(true);
      parser.ExpectedExpression("=");
      this.signature = parser.NextSignature(true, true, 6);
    }

    protected override VistaDBType OnPrepareQuery()
    {
      IParameter parameter = this.DoGetParam(this.name);
      if (parameter == null)
        throw new VistaDBSQLException(616, this.origName, this.lineNo, this.symbolNo);
      this.dataType = parameter.DataType;
      int num = (int) this.signature.Prepare();
      return this.dataType;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      if (this.tempValue == null)
        this.tempValue = this.Database.CreateEmptyColumn(this.dataType);
      this.Database.Conversion.Convert((IValue) this.signature.Execute(), (IValue) this.tempValue);
      this.signature.SetChanged();
      this.DoSetParam(this.name, ((IValue) this.tempValue).Value, this.dataType, this.DoGetParam(this.name).Direction);
      return (IQueryResult) null;
    }
  }
}
