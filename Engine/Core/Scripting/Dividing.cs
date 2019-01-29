using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class Dividing : Signature
  {
    internal Dividing(string name, int groupId)
      : base(name, groupId, Signature.Operations.Nomark, Signature.Priorities.Mutliplication, VistaDBType.Unknown)
    {
      this.AddParameter(VistaDBType.Unknown);
      this.AddParameter(VistaDBType.Unknown);
    }

    internal override Signature DoCloneSignature()
    {
      Signature signature = (Signature) new Dividing(new string(this.Name), this.Group);
      signature.Entry = this.Entry;
      return signature;
    }

    protected override void OnFixReturnTypeAndParameters(Collector collector, int offset, VistaDBType newType)
    {
      VistaDBType returnType1 = collector[offset].Signature.ReturnType;
      VistaDBType returnType2 = collector[offset + 1].Signature.ReturnType;
      this.SetParameterType(0, returnType1);
      this.SetParameterType(1, returnType2);
      base.OnFixReturnTypeAndParameters(collector, offset, Summation.ReturnVistaDBType(returnType1, returnType2));
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      pcode[entry].ResultColumn /= pcode[entry + 1].ResultColumn;
    }
  }
}
