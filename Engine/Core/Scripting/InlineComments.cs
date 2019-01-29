namespace VistaDB.Engine.Core.Scripting
{
  internal class InlineComments : Signature
  {
    internal InlineComments(string name, int groupId)
      : base(name, groupId, Signature.Operations.BgnGroup, Signature.Priorities.MaximumPriority, VistaDBType.Unknown)
    {
    }
  }
}
