using VistaDB.Engine.Core.Scripting;

namespace VistaDB.Engine.Core
{
  internal class FKConstraint : Constraint
  {
    internal FKConstraint(string name, EvalStack evaluation, FilterType typeId)
      : base(name, evaluation, typeId)
    {
    }
  }
}
