using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class NullSignature : Signature
  {
    private static readonly Row.Column Null = (Row.Column) new BitColumn();

    internal NullSignature(string name, int groupId)
      : base(name, groupId, Operations.Null, Priorities.Generator, VistaDBType.Bit)
    {
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      pcode[entry].ResultColumn = Null.Duplicate(false);
    }
  }
}
