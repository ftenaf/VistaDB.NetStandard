using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class NotBitwise : Signature
  {
    internal NotBitwise(string name, int groupId)
      : base(name, groupId, Signature.Operations.Not, Signature.Priorities.Bitwise, VistaDBType.Bit)
    {
      this.AddParameter(VistaDBType.Bit);
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      PCodeUnit pcodeUnit = pcode[entry];
      pcodeUnit.ResultColumn.Value = pcodeUnit.ResultColumn.Value == null ? false : (!(bool)pcodeUnit.ResultColumn.Value ? true : false);
    }
  }
}
