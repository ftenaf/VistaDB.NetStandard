using System;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class GetUtcDateFunction : GetDateFunction
  {
    public GetUtcDateFunction(SQLParser parser)
      : base(parser)
    {
    }

    protected override DateTime EvaluateCurrentMoment()
    {
      return DateTime.UtcNow;
    }
  }
}
