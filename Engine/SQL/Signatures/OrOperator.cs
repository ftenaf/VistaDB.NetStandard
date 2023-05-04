using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class OrOperator : BinaryOperator
  {
    public OrOperator(Signature leftOperand, SQLParser parser)
      : base(leftOperand, parser, 5)
    {
      dataType = VistaDBType.Bit;
    }

    protected override IColumn InternalExecute()
    {
      if (GetIsChanged())
      {
        needsEvaluation = false;
        IColumn column1 = leftOperand.Execute();
        if (!column1.IsNull && (bool) ((IValue) column1).Value)
        {
          ((IValue) result).Value = (object) true;
          return result;
        }
        IColumn column2 = rightOperand.Execute();
        if (!column2.IsNull && (bool) ((IValue) column2).Value)
        {
          ((IValue) result).Value = (object) true;
          return result;
        }
        if (!column1.IsNull && !(bool) ((IValue) column1).Value && (!column2.IsNull && !(bool) ((IValue) column2).Value))
        {
          ((IValue) result).Value = (object) false;
          return result;
        }
        ((IValue) result).Value = (object) null;
      }
      return result;
    }

    public override SignatureType OnPrepare()
    {
      leftOperand = ConstantSignature.PrepareAndCheckConstant(leftOperand, VistaDBType.Bit);
      rightOperand = ConstantSignature.PrepareAndCheckConstant(rightOperand, VistaDBType.Bit);
      if (!leftOperand.AlwaysNull && leftOperand.DataType != VistaDBType.Bit || !rightOperand.AlwaysNull && rightOperand.DataType != VistaDBType.Bit)
        throw new VistaDBSQLException(558, "OR", lineNo, symbolNo);
      optimizable = leftOperand.Optimizable && rightOperand.Optimizable;
      if (leftOperand.AlwaysNull || rightOperand.AlwaysNull || leftOperand.SignatureType == SignatureType.Constant && rightOperand.SignatureType == SignatureType.Constant)
        return SignatureType.Constant;
      return signatureType;
    }

    protected override bool OnOptimize(ConstraintOperations constrainOperations)
    {
      int count = constrainOperations.Count;
      if (!leftOperand.Optimize(constrainOperations))
      {
        constrainOperations.ResetFullOptimizationLevel();
        return false;
      }
      if (rightOperand.Optimize(constrainOperations) && constrainOperations.AddLogicalOr())
        return true;
      constrainOperations.RollBackAddedConstraints(count);
      constrainOperations.ResetFullOptimizationLevel();
      return false;
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
