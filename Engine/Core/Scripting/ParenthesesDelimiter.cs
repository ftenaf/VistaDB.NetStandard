namespace VistaDB.Engine.Core.Scripting
{
  internal class ParenthesesDelimiter : Signature
  {
    internal ParenthesesDelimiter(string name, int groupId, int endOfGroupEntry)
      : base(name, groupId, Signature.Operations.Delimiter, Signature.Priorities.MinPriority, VistaDBType.Unknown, "(", name, ' ', ")", endOfGroupEntry)
    {
    }
  }
}
