using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class StringConversion : Signature
  {
    internal StringConversion(string name, int groupId, int endOfGroupId)
      : base(name, groupId, Signature.Operations.BgnGroup, Signature.Priorities.StdOperator, VistaDBType.NChar, endOfGroupId)
    {
      this.AddParameter(VistaDBType.Unknown);
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      PCodeUnit pcodeUnit = pcode[entry];
      Row.Column resultColumn = pcodeUnit.ResultColumn;
      pcodeUnit.ResultColumn = contextStorage.CreateEmptyColumnInstance(VistaDBType.NChar);
      pcodeUnit.ResultColumn.Value = resultColumn.IsNull ? (object) (string) null : (object) resultColumn.Value.ToString();
    }
  }
}
