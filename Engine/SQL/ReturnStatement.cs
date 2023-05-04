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
      returnSignature = parser.NextSignature(true, false, 6);
    }

    protected override VistaDBType OnPrepareQuery()
    {
      if (returnSignature == null)
        return VistaDBType.Unknown;
      return returnSignature.DataType;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      Batch.ScopeBreakFlag = true;
      IParameter returnParameter = DoGetReturnParameter();
      if (returnSignature == null || returnParameter == null || returnParameter.DataType == VistaDBType.Unknown && parent is StoredFunctionBody)
        return null;
      int num = (int) returnSignature.Prepare();
      returnSignature.SetChanged();
      IColumn column = returnSignature.Execute();
      if (returnParameter.DataType == VistaDBType.Unknown)
        returnParameter.DataType = column.Type;
      IColumn emptyColumn = Database.CreateEmptyColumn(returnParameter.DataType);
      Database.Conversion.Convert(column, emptyColumn);
      returnParameter.Value = emptyColumn.Value;
      return null;
    }
  }
}
