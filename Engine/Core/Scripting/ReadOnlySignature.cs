using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class ReadOnlySignature : Signature
  {
    internal ReadOnlySignature(int groupId, int endOfGroupId)
      : base(Readonly.SystemName, groupId, Signature.Operations.BgnGroup, Signature.Priorities.StdOperator, VistaDBType.Bit, endOfGroupId)
    {
      this.AddParameter(VistaDBType.Unknown);
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      PCodeUnit pcodeUnit = pcode[entry];
      int rowIndex = pcodeUnit.ResultColumn.RowIndex;
      Row.Column column = contextStorage.SatelliteRow[rowIndex];
      pcodeUnit.ResultColumn = (Row.Column) new BitColumn(!column.Edited);
    }
  }
}
