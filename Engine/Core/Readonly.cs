using VistaDB.Engine.Core.Scripting;

namespace VistaDB.Engine.Core
{
  internal class Readonly : Constraint
  {
    internal static readonly string SystemName = "READONLY";

    internal Readonly(string name, EvalStack evaluation)
      : base(name, evaluation, Filter.FilterType.ReadOnly)
    {
    }
  }
}
