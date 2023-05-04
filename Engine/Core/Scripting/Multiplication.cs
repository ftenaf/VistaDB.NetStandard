using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class Multiplication : Signature
  {
    internal Multiplication(string name, int groupId)
      : base(name, groupId, Operations.Nomark, Priorities.Mutliplication, VistaDBType.Unknown)
    {
      AddParameter(VistaDBType.Unknown);
      AddParameter(VistaDBType.Unknown);
    }

    internal override Signature DoCloneSignature()
    {
      Signature signature = (Signature) new Multiplication(new string(Name), Group);
      signature.Entry = Entry;
      return signature;
    }

    protected override void OnFixReturnTypeAndParameters(Collector collector, int offset, VistaDBType newType)
    {
      VistaDBType returnType1 = collector[offset].Signature.ReturnType;
      VistaDBType returnType2 = collector[offset + 1].Signature.ReturnType;
      SetParameterType(0, returnType1);
      SetParameterType(1, returnType2);
      base.OnFixReturnTypeAndParameters(collector, offset, Summation.ReturnVistaDBType(returnType1, returnType2));
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      pcode[entry].ResultColumn *= pcode[entry + 1].ResultColumn;
    }
  }
}
