using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class Great : Equivalence
  {
    internal Great(string name, int groupId)
      : base(name, groupId, Operations.Great)
    {
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      PCodeUnit pcodeUnit1 = pcode[entry];
      PCodeUnit pcodeUnit2 = pcode[entry + 1];
      pcodeUnit1.ResultColumn = new BitColumn(ImplicitCompare(pcodeUnit1.ResultColumn, pcodeUnit2.ResultColumn, contextStorage) > 0L);
    }
  }
}
