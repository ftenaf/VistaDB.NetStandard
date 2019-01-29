using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class IsNull : Signature
  {
    internal IsNull(string name, int groupId)
      : base(name, groupId, Signature.Operations.IsNull, Signature.Priorities.IsBitwise, VistaDBType.Bit)
    {
      this.allowUnaryToFollow = true;
      this.AddParameter(VistaDBType.Unknown);
      this.AddParameter(VistaDBType.Unknown);
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      PCodeUnit pcodeUnit1 = pcode[entry];
      PCodeUnit pcodeUnit2 = pcode[entry + 1];
      Row.Column column1 = (Row.Column) new BitColumn(true);
      Row.Column column2 = !pcodeUnit2.ResultColumn.IsNull ? ((bool) pcodeUnit2.ResultColumn.Value ? (Row.Column) new BitColumn() : (Row.Column) new BitColumn(!pcodeUnit1.ResultColumn.IsNull)) : (Row.Column) new BitColumn(pcodeUnit1.ResultColumn.IsNull);
      pcodeUnit1.ResultColumn = column2;
    }
  }
}
