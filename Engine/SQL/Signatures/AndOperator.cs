using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class AndOperator : BinaryOperator
  {
    public AndOperator(Signature leftOperand, SQLParser parser)
      : base(leftOperand, parser, 4)
    {
      this.dataType = VistaDBType.Bit;
    }

    protected override IColumn InternalExecute()
    {
      if (this.GetIsChanged())
      {
        this.needsEvaluation = false;
        IColumn column1 = this.leftOperand.Execute();
        if (!column1.IsNull && !(bool) ((IValue) column1).Value)
        {
          ((IValue) this.result).Value = (object) false;
          return this.result;
        }
        IColumn column2 = this.rightOperand.Execute();
        if (!column2.IsNull && !(bool) ((IValue) column2).Value)
        {
          ((IValue) this.result).Value = (object) false;
          return this.result;
        }
        if (!column1.IsNull && (bool) ((IValue) column1).Value && (!column2.IsNull && (bool) ((IValue) column2).Value))
        {
          ((IValue) this.result).Value = (object) true;
          return this.result;
        }
        ((IValue) this.result).Value = (object) null;
      }
      return this.result;
    }

    public override SignatureType OnPrepare()
    {
      this.leftOperand = ConstantSignature.PrepareAndCheckConstant(this.leftOperand, VistaDBType.Bit);
      this.rightOperand = ConstantSignature.PrepareAndCheckConstant(this.rightOperand, VistaDBType.Bit);
      if (!this.leftOperand.AlwaysNull && this.leftOperand.DataType != VistaDBType.Bit || !this.rightOperand.AlwaysNull && this.rightOperand.DataType != VistaDBType.Bit)
        throw new VistaDBSQLException(558, "AND", this.lineNo, this.symbolNo);
      this.optimizable = this.leftOperand.Optimizable || this.rightOperand.Optimizable;
      if (this.leftOperand.AlwaysNull || this.rightOperand.AlwaysNull || this.leftOperand.SignatureType == SignatureType.Constant && this.rightOperand.SignatureType == SignatureType.Constant)
        return SignatureType.Constant;
      return this.signatureType;
    }

    protected override bool OnOptimize(ConstraintOperations constrainOperations)
    {
      if (!this.leftOperand.Optimize(constrainOperations))
      {
        constrainOperations.ResetFullOptimizationLevel();
        return this.rightOperand.Optimize(constrainOperations);
      }
      if (this.rightOperand.Optimize(constrainOperations))
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
