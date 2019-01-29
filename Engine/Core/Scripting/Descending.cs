﻿using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class Descending : Signature
  {
    internal Descending(string name, int groupId, int endOfGroupId)
      : base(name, groupId, Signature.Operations.BgnGroup, Signature.Priorities.StdOperator, VistaDBType.Unknown, endOfGroupId)
    {
      this.AddParameter(VistaDBType.Unknown);
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      pcode[entry].ResultColumn.Descending = true;
    }
  }
}
