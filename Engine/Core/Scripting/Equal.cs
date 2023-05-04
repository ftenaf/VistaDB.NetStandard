using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class Equal : Equivalence
  {
    internal Equal(string name, int groupId)
      : base(name, groupId, Operations.Equivalence)
    {
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      PCodeUnit pcodeUnit1 = pcode[entry];
      PCodeUnit pcodeUnit2 = pcode[entry + 1];
      pcodeUnit1.ResultColumn = new BitColumn(ImplicitCompare(pcodeUnit1.ResultColumn, pcodeUnit2.ResultColumn, contextStorage) == 0L);
    }
  }
}
