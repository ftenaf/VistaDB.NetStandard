﻿using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class BreakStatement : Statement
  {
    internal BreakStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      parser.SkipToken(false);
    }

    protected override VistaDBType OnPrepareQuery()
    {
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      WhileStatement cycleStatement = DoGetCycleStatement();
      if (cycleStatement == null)
        throw new VistaDBSQLException(644, null, lineNo, symbolNo);
      cycleStatement.BreakFlag = true;
      return null;
    }
  }
}
