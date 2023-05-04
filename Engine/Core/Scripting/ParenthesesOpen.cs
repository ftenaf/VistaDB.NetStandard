namespace VistaDB.Engine.Core.Scripting
{
  internal class ParenthesesOpen : Signature
  {
    internal ParenthesesOpen(string name, int groupId, int endOfGroupEntry)
      : base(name, groupId, Operations.BgnGroup, Priorities.MinPriority, VistaDBType.Unknown, null, ",", ' ', ")", endOfGroupEntry)
    {
    }
  }
}
