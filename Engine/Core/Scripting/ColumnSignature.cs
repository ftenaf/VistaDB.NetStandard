using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class ColumnSignature : Signature
  {
    internal ColumnSignature(int groupId, VistaDBType type)
      : base("Column", groupId, Operations.Nomark, Priorities.Generator, type)
    {
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      PCodeUnit pcodeUnit = pcode[entry];
      int rowIndex = pcodeUnit.ResultColumn.RowIndex;
      pcodeUnit.ResultColumn = contextRow[pcodeUnit.ResultColumn.RowIndex].Duplicate(false);
      pcodeUnit.ResultColumn.RowIndex = rowIndex;
    }
  }
}
