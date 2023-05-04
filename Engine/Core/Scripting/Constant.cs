using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class Constant : Signature
  {
    private Row.Column constant;

    internal Constant(int groupId)
      : base(null, groupId, Operations.Nomark, Priorities.Generator, VistaDBType.Unknown)
    {
    }

    internal Row.Column ConstantColumn
    {
      get
      {
        return constant;
      }
      set
      {
        constant = value == null ? null : value.Duplicate(false);
        SetParameterType(-1, value.Type);
      }
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      pcode[entry].ResultColumn.Value = constant.Value;
    }

    internal override Signature DoCloneSignature()
    {
      return new Constant(Group);
    }
  }
}
