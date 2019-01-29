namespace VistaDB.Engine.Core.Scripting
{
  internal class EndComments : Signature
  {
    internal EndComments(string bgnName, string endName, int groupId)
      : base(bgnName, groupId, Signature.Operations.EndGroup, Signature.Priorities.MaximumPriority, VistaDBType.Unknown, bgnName, " ", ' ', endName)
    {
    }
  }
}
