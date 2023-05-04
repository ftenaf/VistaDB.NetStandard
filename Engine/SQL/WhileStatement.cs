using System;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;
using VistaDB.Provider;

namespace VistaDB.Engine.SQL
{
  internal class WhileStatement : BatchStatement
  {
    private Signature conditionSignature;

    internal WhileStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      conditionSignature = parser.NextSignature(true, true, 6);
      if (parser.IsToken("BEGIN"))
      {
        parser.SkipToken(true);
        while (!parser.SkipSemicolons() && !parser.IsToken("END"))
          Add(connection.ParseStatement((Statement) this, id));
        parser.SkipToken(false);
      }
      else
        Add(connection.ParseStatement((Statement) this, id));
    }

    protected override VistaDBType OnPrepareQuery()
    {
      int num = (int) conditionSignature.OnPrepare();
      if (conditionSignature.DataType != VistaDBType.Bit)
        throw new Exception("An expression of non-boolean type specified in a context where a condition is expected, near 'SELECT'");
      return base.OnPrepareQuery();
    }

    public override WhileStatement DoGetCycleStatement()
    {
      return this;
    }

    public override INextQueryResult NextResult(VistaDBPipe pipe)
    {
      if (currentStatement == 0 && !ExecConditionSignature())
      {
        currentStatement = -1;
        return (INextQueryResult) null;
      }
      INextQueryResult nextQueryResult = base.NextResult(pipe);
      if (currentStatement >= statements.Count)
        currentStatement = 0;
      return nextQueryResult;
    }

    internal void Continue()
    {
      currentStatement = -1;
    }

    private bool ExecConditionSignature()
    {
      conditionSignature.SetChanged();
      IColumn column = conditionSignature.Execute();
      if (column != null && !column.IsNull)
        return (bool) ((IValue) column).Value;
      return false;
    }
  }
}
