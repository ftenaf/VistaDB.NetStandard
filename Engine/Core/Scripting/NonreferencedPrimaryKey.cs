using VistaDB.DDA;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class NonreferencedPrimaryKey : Signature
  {
    internal static readonly string NonReferencedKey = "PRIMARY KEY REFERENCED ";

    internal NonreferencedPrimaryKey(int groupId, int endOfGroupId)
      : base(NonreferencedPrimaryKey.NonReferencedKey, groupId, Signature.Operations.BgnGroup, Signature.Priorities.StdOperator, VistaDBType.Bit, endOfGroupId)
    {
      this.AddParameter(VistaDBType.NChar);
      this.AddParameter(VistaDBType.NChar);
      this.AddParameter(VistaDBType.SmallInt);
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      PCodeUnit pcodeUnit1 = pcode[entry];
      PCodeUnit pcodeUnit2 = pcode[entry + 1];
      PCodeUnit pcodeUnit3 = pcode[entry + 2];
      bool flag = ((VistaDB.Engine.Core.Indexing.Index) contextStorage).DoCheckUnlinkedPrimaryKey(pcodeUnit1.ResultColumn.Value.ToString(), pcodeUnit2.ResultColumn.Value.ToString(), (VistaDBReferentialIntegrity) (short) pcodeUnit3.ResultColumn.Value);
      pcodeUnit1.ResultColumn = flag ? TrueSignature.True : FalseSignature.False;
    }
  }
}
