using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class FalseSignature : Signature
  {
    internal static readonly Row.Column False = (Row.Column) new BitColumn(false);

    internal FalseSignature(string name, int groupId)
      : base(name, groupId, Operations.False, Priorities.Generator, VistaDBType.Bit)
    {
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      pcode[entry].ResultColumn = False.Duplicate(false);
    }
  }
}
