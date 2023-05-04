using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class IsNull : Signature
  {
    internal IsNull(string name, int groupId)
      : base(name, groupId, Operations.IsNull, Priorities.IsBitwise, VistaDBType.Bit)
    {
      allowUnaryToFollow = true;
      AddParameter(VistaDBType.Unknown);
      AddParameter(VistaDBType.Unknown);
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      PCodeUnit pcodeUnit1 = pcode[entry];
      PCodeUnit pcodeUnit2 = pcode[entry + 1];
      Row.Column column1 = new BitColumn(true);
      Row.Column column2 = !pcodeUnit2.ResultColumn.IsNull ? ((bool) pcodeUnit2.ResultColumn.Value ? new BitColumn() : (Row.Column) new BitColumn(!pcodeUnit1.ResultColumn.IsNull)) : new BitColumn(pcodeUnit1.ResultColumn.IsNull);
      pcodeUnit1.ResultColumn = column2;
    }
  }
}
