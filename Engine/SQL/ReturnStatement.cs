using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal class ReturnStatement : Statement
  {
    private Signature returnSignature;

    internal ReturnStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      this.returnSignature = parser.NextSignature(true, false, 6);
    }

    protected override VistaDBType OnPrepareQuery()
    {
      if (this.returnSignature == (Signature) null)
        return VistaDBType.Unknown;
      return this.returnSignature.DataType;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      this.Batch.ScopeBreakFlag = true;
      IParameter returnParameter = this.DoGetReturnParameter();
      if (this.returnSignature == (Signature) null || returnParameter == null || returnParameter.DataType == VistaDBType.Unknown && this.parent is StoredFunctionBody)
        return (IQueryResult) null;
      int num = (int) this.returnSignature.Prepare();
      this.returnSignature.SetChanged();
      IColumn column = this.returnSignature.Execute();
      if (returnParameter.DataType == VistaDBType.Unknown)
        returnParameter.DataType = column.Type;
      IColumn emptyColumn = this.Database.CreateEmptyColumn(returnParameter.DataType);
      this.Database.Conversion.Convert((IValue) column, (IValue) emptyColumn);
      returnParameter.Value = ((IValue) emptyColumn).Value;
      return (IQueryResult) null;
    }
  }
}
