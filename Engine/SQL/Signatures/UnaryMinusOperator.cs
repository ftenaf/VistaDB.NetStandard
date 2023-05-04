using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class UnaryMinusOperator : UnaryArithmeticOperator
  {
    public UnaryMinusOperator(SQLParser parser)
      : base(parser, 1)
    {
    }

    protected override IColumn InternalExecute()
    {
      if (GetIsChanged())
      {
        Row.Column column = -(Row.Column) operand.Execute();
        if (result == null)
          result = CreateColumn(column.Type);
        ((IValue) result).Value = column.Value;
        needsEvaluation = false;
      }
      return result;
    }
  }
}
