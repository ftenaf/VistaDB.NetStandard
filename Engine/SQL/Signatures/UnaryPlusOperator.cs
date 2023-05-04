﻿using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class UnaryPlusOperator : UnaryArithmeticOperator
  {
    public UnaryPlusOperator(SQLParser parser)
      : base(parser, 1)
    {
    }

    protected override IColumn InternalExecute()
    {
      if (GetIsChanged())
      {
        ((IValue) result).Value = ((IValue) operand.Execute()).Value;
        needsEvaluation = false;
      }
      return result;
    }
  }
}
