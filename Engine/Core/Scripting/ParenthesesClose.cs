namespace VistaDB.Engine.Core.Scripting
{
  internal class ParenthesesClose : Signature
  {
    internal ParenthesesClose(string name, int groupId, int endOfGroupEntry)
      : base(name, groupId, Operations.EndGroup, Priorities.MinPriority, VistaDBType.Unknown, "(", ",", ' ', name, endOfGroupEntry)
    {
    }
  }
}
