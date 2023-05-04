using System;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class MoneyConversion : Signature
  {
    internal MoneyConversion(string name, int groupId, int endOfGroupId)
      : base(name, groupId, Operations.BgnGroup, Priorities.StdOperator, VistaDBType.Money, endOfGroupId)
    {
      AddParameter(VistaDBType.Unknown);
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      PCodeUnit pcodeUnit = pcode[entry];
      pcodeUnit.ResultColumn = pcodeUnit.ResultColumn.IsNull ? new MoneyColumn() : (Row.Column) new MoneyColumn(Decimal.Parse(pcodeUnit.ResultColumn.Value.ToString()));
    }
  }
}
