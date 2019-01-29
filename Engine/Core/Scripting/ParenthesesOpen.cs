namespace VistaDB.Engine.Core.Scripting
{
  internal class ParenthesesOpen : Signature
  {
    internal ParenthesesOpen(string name, int groupId, int endOfGroupEntry)
      : base(name, groupId, Signature.Operations.BgnGroup, Signature.Priorities.MinPriority, VistaDBType.Unknown, (string) null, ",", ' ', ")", endOfGroupEntry)
    {
    }
  }
}
