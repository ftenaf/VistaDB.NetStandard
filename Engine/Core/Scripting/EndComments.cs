namespace VistaDB.Engine.Core.Scripting
{
  internal class EndComments : Signature
  {
    internal EndComments(string bgnName, string endName, int groupId)
      : base(bgnName, groupId, Operations.EndGroup, Priorities.MaximumPriority, VistaDBType.Unknown, bgnName, " ", ' ', endName)
    {
    }
  }
}
