using VistaDB.Engine.Core.Scripting;
using VistaDB.Engine.SQL;

namespace VistaDB.Engine.Core
{
  internal class SQLConstraint : Constraint
  {
    private CheckStatement checkConstraint;
    private string expression;

    internal SQLConstraint(string name, Filter.FilterType typeId, CheckStatement checkConstraint, string expression)
      : base(name, (EvalStack) null, typeId)
    {
      this.checkConstraint = checkConstraint;
      this.expression = expression;
    }

    protected override bool OnGetValidRowStatus(Row row)
    {
      return this.checkConstraint.Evaluate(row);
    }

    internal override string Expression
    {
      get
      {
        return this.expression;
      }
    }
  }
}
