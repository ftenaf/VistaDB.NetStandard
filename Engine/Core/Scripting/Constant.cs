using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class Constant : Signature
  {
    private Row.Column constant;

    internal Constant(int groupId)
      : base((string) null, groupId, Signature.Operations.Nomark, Signature.Priorities.Generator, VistaDBType.Unknown)
    {
    }

    internal Row.Column ConstantColumn
    {
      get
      {
        return this.constant;
      }
      set
      {
        this.constant = value == (Row.Column) null ? (Row.Column) null : value.Duplicate(false);
        this.SetParameterType(-1, value.Type);
      }
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      pcode[entry].ResultColumn.Value = this.constant.Value;
    }

    internal override Signature DoCloneSignature()
    {
      return (Signature) new Constant(this.Group);
    }
  }
}
