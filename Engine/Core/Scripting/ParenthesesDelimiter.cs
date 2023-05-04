namespace VistaDB.Engine.Core.Scripting
{
  internal class ParenthesesDelimiter : Signature
  {
    internal ParenthesesDelimiter(string name, int groupId, int endOfGroupEntry)
      : base(name, groupId, Operations.Delimiter, Priorities.MinPriority, VistaDBType.Unknown, "(", name, ' ', ")", endOfGroupEntry)
    {
    }
  }
}
