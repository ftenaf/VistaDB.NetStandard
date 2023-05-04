using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class NotBitwise : Signature
  {
    internal NotBitwise(string name, int groupId)
      : base(name, groupId, Operations.Not, Priorities.Bitwise, VistaDBType.Bit)
    {
      AddParameter(VistaDBType.Bit);
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      PCodeUnit pcodeUnit = pcode[entry];
      pcodeUnit.ResultColumn.Value = pcodeUnit.ResultColumn.Value == null ? false : (!(bool)pcodeUnit.ResultColumn.Value ? true : false);
    }
  }
}
