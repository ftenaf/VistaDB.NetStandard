using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class TrueSignature : Signature
  {
    internal static readonly Row.Column True = (Row.Column) new BitColumn(true);

    internal TrueSignature(string name, int groupId)
      : base(name, groupId, Operations.True, Priorities.Generator, VistaDBType.Bit)
    {
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      pcode[entry].ResultColumn = True.Duplicate(false);
    }
  }
}
