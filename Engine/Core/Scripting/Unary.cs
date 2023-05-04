namespace VistaDB.Engine.Core.Scripting
{
  internal class Unary : Signature
  {
    internal Unary(string name, int groupId)
      : base(name, groupId, Operations.Nomark, Priorities.UnarySummation, VistaDBType.Unknown)
    {
      AddParameter(VistaDBType.Unknown);
    }

    protected override void OnFixReturnTypeAndParameters(Collector collector, int offset, VistaDBType newType)
    {
      VistaDBType returnType = collector[offset].Signature.ReturnType;
      SetParameterType(0, returnType);
      base.OnFixReturnTypeAndParameters(collector, offset, returnType);
    }
  }
}
