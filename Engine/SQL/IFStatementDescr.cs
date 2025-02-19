﻿using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class IFStatementDescr : IStatementDescr
  {
    public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
    {
      return new IFStatement(conn, parent, parser, id);
    }
  }
}
