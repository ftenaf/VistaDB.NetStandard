using VistaDB.Engine.Core.Scripting;

namespace VistaDB.Engine.Core
{
  internal class OrdinaryFilter : Filter
  {
    internal OrdinaryFilter(EvalStack evaluation)
      : base(evaluation, FilterType.Ordinary, true, true, 0)
    {
    }
  }
}
