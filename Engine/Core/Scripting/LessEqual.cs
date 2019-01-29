using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class LessEqual : Equivalence
  {
    internal LessEqual(string name, int groupId)
      : base(name, groupId, Signature.Operations.LessEqual)
    {
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      PCodeUnit pcodeUnit1 = pcode[entry];
      PCodeUnit pcodeUnit2 = pcode[entry + 1];
      pcodeUnit1.ResultColumn = (Row.Column) new BitColumn(this.ImplicitCompare(pcodeUnit1.ResultColumn, pcodeUnit2.ResultColumn, contextStorage) <= 0L);
    }
  }
}
