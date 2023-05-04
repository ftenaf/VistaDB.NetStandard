using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class AndOperator : BinaryOperator
  {
    public AndOperator(Signature leftOperand, SQLParser parser)
      : base(leftOperand, parser, 4)
    {
      dataType = VistaDBType.Bit;
    }

    protected override IColumn InternalExecute()
    {
      if (GetIsChanged())
      {
        needsEvaluation = false;
        IColumn column1 = leftOperand.Execute();
        if (!column1.IsNull && !(bool)column1.Value)
        {
                    result.Value = false;
          return result;
        }
        IColumn column2 = rightOperand.Execute();
        if (!column2.IsNull && !(bool)column2.Value)
        {
                    result.Value = false;
          return result;
        }
        if (!column1.IsNull && (bool)column1.Value && (!column2.IsNull && (bool)column2.Value))
        {
                    result.Value = true;
          return result;
        }
                result.Value = null;
      }
      return result;
    }

    public override SignatureType OnPrepare()
    {
      leftOperand = ConstantSignature.PrepareAndCheckConstant(leftOperand, VistaDBType.Bit);
      rightOperand = ConstantSignature.PrepareAndCheckConstant(rightOperand, VistaDBType.Bit);
      if (!leftOperand.AlwaysNull && leftOperand.DataType != VistaDBType.Bit || !rightOperand.AlwaysNull && rightOperand.DataType != VistaDBType.Bit)
        throw new VistaDBSQLException(558, "AND", lineNo, symbolNo);
      optimizable = leftOperand.Optimizable || rightOperand.Optimizable;
      if (leftOperand.AlwaysNull || rightOperand.AlwaysNull || leftOperand.SignatureType == SignatureType.Constant && rightOperand.SignatureType == SignatureType.Constant)
        return SignatureType.Constant;
      return signatureType;
    }

    protected override bool OnOptimize(ConstraintOperations constrainOperations)
    {
      if (!leftOperand.Optimize(constrainOperations))
      {
        constrainOperations.ResetFullOptimizationLevel();
        return rightOperand.Optimize(constrainOperations);
      }
      if (rightOperand.Optimize(constrainOperations))
        return constrainOperations.AddLogicalAnd();
      constrainOperations.ResetFullOptimizationLevel();
      return true;
    }

    public override bool AlwaysNull
    {
      get
      {
        return false;
      }
    }
  }
}
