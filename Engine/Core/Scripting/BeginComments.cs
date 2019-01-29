namespace VistaDB.Engine.Core.Scripting
{
  internal class BeginComments : Signature
  {
    internal BeginComments(string bgnName, string endName, int groupId)
      : base(bgnName, groupId, Signature.Operations.BgnGroup, Signature.Priorities.MaximumPriority, VistaDBType.Unknown, "", " ", ' ', endName, groupId + 1)
    {
    }
  }
}
