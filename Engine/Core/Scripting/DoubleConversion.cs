﻿using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class DoubleConversion : Signature
  {
    internal DoubleConversion(string name, int groupId, int endOfGroupId)
      : base(name, groupId, Operations.BgnGroup, Priorities.StdOperator, VistaDBType.Float, endOfGroupId)
    {
      AddParameter(VistaDBType.Unknown);
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      PCodeUnit pcodeUnit = pcode[entry];
      pcodeUnit.ResultColumn = pcodeUnit.ResultColumn.IsNull ? new FloatColumn() : (Row.Column) new FloatColumn(double.Parse(pcodeUnit.ResultColumn.Value.ToString(), CrossConversion.NumberFormat));
    }
  }
}
