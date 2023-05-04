namespace VistaDB.Engine.Core.Scripting
{
  internal class InlineComments : Signature
  {
    internal InlineComments(string name, int groupId)
      : base(name, groupId, Operations.BgnGroup, Priorities.MaximumPriority, VistaDBType.Unknown)
    {
    }
  }
}
