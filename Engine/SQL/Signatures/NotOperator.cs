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
      if (this.GetIsChanged())
      {
        IColumn column = this.operand.Execute();
        if (column.IsNull)
          ((IValue) this.result).Value = (object) null;
        else
          ((IValue) this.result).Value = (object) !(bool) ((IValue) column).Value;
        this.needsEvaluation = false;
      }
      return this.result;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      this.dataType = VistaDBType.Bit;
      this.optimizable = this.operand.Optimizable;
      if (this.AlwaysNull)
        return SignatureType.Constant;
      if (this.operand.DataType != VistaDBType.Bit)
        throw new VistaDBSQLException(558, "NOT", this.lineNo, this.symbolNo);
      return signatureType;
    }

    protected override bool OnOptimize(ConstraintOperations constrainOperations)
    {
      if (this.operand.Optimize(constrainOperations))
        return constrainOperations.AddLogicalNot();
      return false;
    }

    public override bool IsNull
    {
      get
      {
        return this.operand.IsNull;
      }
    }
  }
}
