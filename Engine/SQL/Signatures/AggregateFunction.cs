using System.Collections.Generic;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal abstract class AggregateFunction : Signature
  {
    protected Signature expression;
    protected object val;
    protected bool distinct;
    protected bool all;
    private bool changed;
    protected bool countOptimized;

    public AggregateFunction(SQLParser parser, bool allowAll)
      : base(parser)
    {
      parser.SkipToken(true);
      if (!parser.IsToken("("))
        throw new VistaDBSQLException(500, "\"(\"", this.lineNo, this.symbolNo);
      this.distinct = false;
      parser.SkipToken(true);
      if (allowAll && parser.IsToken("*"))
      {
        this.expression = (Signature) null;
        parser.SkipToken(true);
      }
      else
      {
        if (parser.TokenEndsWith("*"))
          throw new VistaDBSQLException(656, "", this.lineNo, this.symbolNo);
        if (parser.IsToken("DISTINCT"))
        {
          this.distinct = true;
          parser.SkipToken(true);
          if (parser.TokenEndsWith("*"))
            throw new VistaDBSQLException(658, "", this.lineNo, this.symbolNo);
        }
        else if (parser.IsToken("ALL"))
        {
          parser.SkipToken(true);
          if (parser.TokenEndsWith("*"))
            throw new VistaDBSQLException(657, "", this.lineNo, this.symbolNo);
        }
        this.expression = parser.NextSignature(false, true, 6);
        bool distinct;
        if (this.expression.HasAggregateFunction(out distinct))
          throw new VistaDBSQLException(551, "", this.lineNo, this.symbolNo);
      }
      parser.ExpectedExpression(")");
      this.signatureType = SignatureType.Expression;
      this.val = (object) null;
      this.all = this.expression == (Signature) null;
      this.changed = false;
      this.countOptimized = false;
    }

    protected override IColumn InternalExecute()
    {
      return this.result;
    }

    protected override bool IsEquals(Signature signature)
    {
      if (this.GetType() != signature.GetType())
        return false;
      AggregateFunction aggregateFunction = (AggregateFunction) signature;
      if (this.expression == aggregateFunction.expression)
        return this.distinct == aggregateFunction.distinct;
      return false;
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
    }

    public override void SetChanged()
    {
      if (this.expression != (Signature) null)
        this.expression.SetChanged();
      this.changed = false;
    }

    public override void ClearChanged()
    {
      if (this.expression != (Signature) null)
        this.expression.ClearChanged();
      this.changed = false;
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      distinct = this.distinct;
      return true;
    }

    public override SignatureType OnPrepare()
    {
      if (this.expression != (Signature) null)
        this.expression = ConstantSignature.PrepareAndCheckConstant(this.expression, VistaDBType.Unknown);
      return this.signatureType;
    }

    public override bool AlwaysNull
    {
      get
      {
        return false;
      }
    }

    protected override bool InternalGetIsChanged()
    {
      if (!this.changed && !(this.expression == (Signature) null))
        return this.expression.GetIsChanged();
      return true;
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
      list.Add(this);
    }

    public override int ColumnCount
    {
      get
      {
        return 0;
      }
    }

    public bool Distinct
    {
      get
      {
        return this.distinct;
      }
    }

    public Signature Expression
    {
      get
      {
        return this.expression;
      }
    }

    public void CreateEmptyResult()
    {
      if (this.result == null)
        this.result = this.CreateColumn(this.dataType);
      this.val = this.InternalCreateEmptyResult();
      ((IValue) this.result).Value = this.val;
      this.changed = true;
    }

    public void CreateNewGroup(object newVal)
    {
      if (this.result == null)
        this.result = this.CreateColumn(this.dataType);
      this.val = this.InternalCreateNewGroup(newVal);
      this.changed = true;
    }

    public bool AddRowToGroup(object newVal)
    {
      this.val = this.InternalAddRowToGroup(newVal);
      return !this.countOptimized;
    }

    public void FinishGroup()
    {
      this.val = this.InternalFinishGroup();
      ((IValue) this.result).Value = this.val;
    }

    public void CreateNewGroupAndSerialize(object newVal, ref object serObj)
    {
      this.CreateNewGroup(newVal);
      this.InternalSerialize(ref serObj);
    }

    public void AddRowToGroupAndSerialize(object newVal, ref object serObj)
    {
      this.InternalDeserialize(serObj);
      this.AddRowToGroup(newVal);
      this.InternalSerialize(ref serObj);
    }

    public void FinishGroup(object serObj)
    {
      this.InternalDeserialize(serObj);
      this.FinishGroup();
    }

    protected abstract void InternalSerialize(ref object serObj);

    protected abstract void InternalDeserialize(object serObj);

    protected abstract object InternalCreateEmptyResult();

    protected abstract object InternalCreateNewGroup(object newVal);

    protected abstract object InternalAddRowToGroup(object newVal);

    protected abstract object InternalFinishGroup();
  }
}
