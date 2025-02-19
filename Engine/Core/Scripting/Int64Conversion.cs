﻿using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class Int64Conversion : Signature
  {
    internal Int64Conversion(string name, int groupId, int endOfGroupId)
      : base(name, groupId, Operations.BgnGroup, Priorities.StdOperator, VistaDBType.BigInt, endOfGroupId)
    {
      AddParameter(VistaDBType.Unknown);
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      PCodeUnit pcodeUnit = pcode[entry];
      pcodeUnit.ResultColumn = pcodeUnit.ResultColumn.IsNull ? new BigIntColumn() : (Row.Column) new BigIntColumn(long.Parse(pcodeUnit.ResultColumn.Value.ToString()));
    }
  }
}
