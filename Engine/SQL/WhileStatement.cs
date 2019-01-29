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
      this.conditionSignature = parser.NextSignature(true, true, 6);
      if (parser.IsToken("BEGIN"))
      {
        parser.SkipToken(true);
        while (!parser.SkipSemicolons() && !parser.IsToken("END"))
          this.Add(connection.ParseStatement((Statement) this, this.id));
        parser.SkipToken(false);
      }
      else
        this.Add(connection.ParseStatement((Statement) this, this.id));
    }

    protected override VistaDBType OnPrepareQuery()
    {
      int num = (int) this.conditionSignature.OnPrepare();
      if (this.conditionSignature.DataType != VistaDBType.Bit)
        throw new Exception("An expression of non-boolean type specified in a context where a condition is expected, near 'SELECT'");
      return base.OnPrepareQuery();
    }

    public override WhileStatement DoGetCycleStatement()
    {
      return this;
    }

    public override INextQueryResult NextResult(VistaDBPipe pipe)
    {
      if (this.currentStatement == 0 && !this.ExecConditionSignature())
      {
        this.currentStatement = -1;
        return (INextQueryResult) null;
      }
      INextQueryResult nextQueryResult = base.NextResult(pipe);
      if (this.currentStatement >= this.statements.Count)
        this.currentStatement = 0;
      return nextQueryResult;
    }

    internal void Continue()
    {
      this.currentStatement = -1;
    }

    private bool ExecConditionSignature()
    {
      this.conditionSignature.SetChanged();
      IColumn column = this.conditionSignature.Execute();
      if (column != null && !column.IsNull)
        return (bool) ((IValue) column).Value;
      return false;
    }
  }
}
