namespace VistaDB.Engine.Core.Scripting
{
  internal class Break : Signature
  {
    internal Break(int groupId)
      : base((string) null, groupId, Operations.Nomark, Priorities.MaximumPriority, VistaDBType.Unknown)
    {
      allowUnaryToFollow = true;
    }
  }
}
