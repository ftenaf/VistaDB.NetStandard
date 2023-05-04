using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class NotOperator : UnaryOperator
  {
    public NotOperator(SQLParser parser)
      : base(parser, 3)
    {
    }

    protected override IColumn InternalExecute()
    {
      if (GetIsChanged())
      {
        IColumn column = operand.Execute();
        if (column.IsNull)
          ((IValue) result).Value = (object) null;
        else
          ((IValue) result).Value = (object) !(bool) ((IValue) column).Value;
        needsEvaluation = false;
      }
      return result;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      dataType = VistaDBType.Bit;
      optimizable = operand.Optimizable;
      if (AlwaysNull)
        return SignatureType.Constant;
      if (operand.DataType != VistaDBType.Bit)
        throw new VistaDBSQLException(558, "NOT", lineNo, symbolNo);
      return signatureType;
    }

    protected override bool OnOptimize(ConstraintOperations constrainOperations)
    {
      if (operand.Optimize(constrainOperations))
        return constrainOperations.AddLogicalNot();
      return false;
    }

    public override bool IsNull
    {
      get
      {
        return operand.IsNull;
      }
    }
  }
}
