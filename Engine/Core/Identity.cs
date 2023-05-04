using VistaDB.Engine.Core.Scripting;

namespace VistaDB.Engine.Core
{
  internal class Identity : Filter
  {
    internal static readonly string SystemName = "IDENTITY";

    internal Identity(EvalStack evaluation)
      : base(evaluation, FilterType.Identity, true, true, 4)
    {
    }
  }
}
