namespace VistaDB.Engine.Core.Scripting
{
  internal class Break : Signature
  {
    internal Break(int groupId)
      : base((string) null, groupId, Signature.Operations.Nomark, Signature.Priorities.MaximumPriority, VistaDBType.Unknown)
    {
      this.allowUnaryToFollow = true;
    }
  }
}
