using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class IdentitySignature : Signature
  {
    internal IdentitySignature(int groupId, int endOfGroupId)
      : base(Identity.SystemName, groupId, Operations.BgnGroup, Priorities.StdOperator, VistaDBType.Bit, endOfGroupId)
    {
      AddParameter(VistaDBType.Unknown);
      AddParameter(VistaDBType.Unknown);
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      PCodeUnit pcodeUnit1 = pcode[entry];
      PCodeUnit pcodeUnit2 = pcode[entry + 1];
      int rowIndex = pcodeUnit1.ResultColumn.RowIndex;
      Row defaultRow = contextStorage.DefaultRow;
      Row.Column column1 = defaultRow[rowIndex];
      contextRow[rowIndex].Value = column1.Value;
      Row.Column column2 = column1 + pcodeUnit2.ResultColumn;
      ++defaultRow.RowVersion;
      pcodeUnit1.ResultColumn = TrueSignature.True;
    }
  }
}
