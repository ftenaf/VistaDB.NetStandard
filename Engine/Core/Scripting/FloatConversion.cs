using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class FloatConversion : Signature
  {
    internal FloatConversion(string name, int groupId, int endOfGroupId)
      : base(name, groupId, Operations.BgnGroup, Priorities.StdOperator, VistaDBType.Real, endOfGroupId)
    {
      AddParameter(VistaDBType.Unknown);
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      PCodeUnit pcodeUnit = pcode[entry];
      pcodeUnit.ResultColumn = pcodeUnit.ResultColumn.IsNull ? new RealColumn() : (Row.Column) new RealColumn(float.Parse(pcodeUnit.ResultColumn.Value.ToString(), CrossConversion.NumberFormat));
    }
  }
}
