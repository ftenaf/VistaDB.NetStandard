using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class ForeignKeyConstraint : Signature
  {
    internal static readonly string ReferencedKey = "FOREIGN KEY REFERENCES ";

    internal ForeignKeyConstraint(int groupId, int endOfGroupId)
      : base(ForeignKeyConstraint.ReferencedKey, groupId, Signature.Operations.BgnGroup, Signature.Priorities.StdOperator, VistaDBType.Bit, endOfGroupId)
    {
      this.AddParameter(VistaDBType.NChar);
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      PCodeUnit pcodeUnit = pcode[entry];
      bool flag = ((VistaDB.Engine.Core.Indexing.Index) contextStorage).DoCheckLinkedForeignKey(pcodeUnit.ResultColumn.Value.ToString());
      pcodeUnit.ResultColumn = flag ? TrueSignature.True : FalseSignature.False;
    }
  }
}
