namespace VistaDB.Engine.Core.Scripting
{
  internal class ParenthesesClose : Signature
  {
    internal ParenthesesClose(string name, int groupId, int endOfGroupEntry)
      : base(name, groupId, Signature.Operations.EndGroup, Signature.Priorities.MinPriority, VistaDBType.Unknown, "(", ",", ' ', name, endOfGroupEntry)
    {
    }
  }
}
