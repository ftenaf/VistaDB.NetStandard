using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class AndBitwise : Signature
  {
    internal AndBitwise(string name, int groupId)
      : base(name, groupId, Operations.And, Priorities.Bitwise, VistaDBType.Bit)
    {
      AddParameter(VistaDBType.Bit);
      AddParameter(VistaDBType.Bit);
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      PCodeUnit pcodeUnit1 = pcode[entry];
      PCodeUnit pcodeUnit2 = pcode[entry + 1];
      bool flag1 = (bool) (pcodeUnit1.ResultColumn.IsNull ? (object) false : pcodeUnit1.ResultColumn.Value);
      bool flag2 = (bool) (pcodeUnit2.ResultColumn.IsNull ? (object) false : pcodeUnit2.ResultColumn.Value);
      pcodeUnit1.ResultColumn.Value = !flag1 ? false : (flag2 ? true : false);
    }
  }
}
