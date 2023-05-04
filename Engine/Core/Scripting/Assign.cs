using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class Assign : Signature
  {
    internal Assign(string name, int groupId, VistaDBType type)
      : base(name, groupId, Operations.Nomark, Priorities.Setting, VistaDBType.Bit)
    {
      allowUnaryToFollow = true;
      AddParameter(type);
      AddParameter(VistaDBType.Unknown);
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      PCodeUnit pcodeUnit1 = pcode[entry];
      PCodeUnit pcodeUnit2 = pcode[entry + 1];
      Row.Column column = contextRow[pcodeUnit1.ResultColumn.RowIndex];
      Row.Column resultColumn = pcodeUnit2.ResultColumn;
      contextStorage.Conversion.Convert((IValue) resultColumn, (IValue) column);
      pcodeUnit1.ResultColumn = TrueSignature.True;
    }
  }
}
