namespace VistaDB.Engine.Core.Scripting
{
  internal class BeginComments : Signature
  {
    internal BeginComments(string bgnName, string endName, int groupId)
      : base(bgnName, groupId, Operations.BgnGroup, Priorities.MaximumPriority, VistaDBType.Unknown, "", " ", ' ', endName, groupId + 1)
    {
    }
  }
}
