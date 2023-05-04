using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class StringConversion : Signature
  {
    internal StringConversion(string name, int groupId, int endOfGroupId)
      : base(name, groupId, Operations.BgnGroup, Priorities.StdOperator, VistaDBType.NChar, endOfGroupId)
    {
      AddParameter(VistaDBType.Unknown);
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      PCodeUnit pcodeUnit = pcode[entry];
      Row.Column resultColumn = pcodeUnit.ResultColumn;
      pcodeUnit.ResultColumn = contextStorage.CreateEmptyColumnInstance(VistaDBType.NChar);
      pcodeUnit.ResultColumn.Value = resultColumn.IsNull ? null : (object) resultColumn.Value.ToString();
    }
  }
}
