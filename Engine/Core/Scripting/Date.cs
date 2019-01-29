using System;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class Date : Signature
  {
    internal Date(string name, int groupId, int endOfGroupId)
      : base(name, groupId, Signature.Operations.BgnGroup, Signature.Priorities.StdOperator, VistaDBType.DateTime, endOfGroupId)
    {
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      pcode[entry].ResultColumn = (Row.Column) new DateTimeColumn(DateTime.Now.Date);
    }
  }
}
