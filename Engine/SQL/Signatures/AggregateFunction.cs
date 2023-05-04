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
        throw new VistaDBSQLException(500, "\"(\"", lineNo, symbolNo);
      distinct = false;
      parser.SkipToken(true);
      if (allowAll && parser.IsToken("*"))
      {
        expression = null;
        parser.SkipToken(true);
      }
      else
      {
        if (parser.TokenEndsWith("*"))
          throw new VistaDBSQLException(656, "", lineNo, symbolNo);
        if (parser.IsToken("DISTINCT"))
        {
          this.distinct = true;
          parser.SkipToken(true);
          if (parser.TokenEndsWith("*"))
            throw new VistaDBSQLException(658, "", lineNo, symbolNo);
        }
        else if (parser.IsToken("ALL"))
        {
          parser.SkipToken(true);
          if (parser.TokenEndsWith("*"))
            throw new VistaDBSQLException(657, "", lineNo, symbolNo);
        }
        expression = parser.NextSignature(false, true, 6);
        bool distinct;
        if (expression.HasAggregateFunction(out distinct))
          throw new VistaDBSQLException(551, "", lineNo, symbolNo);
      }
      parser.ExpectedExpression(")");
      signatureType = SignatureType.Expression;
      val = null;
      all = expression == null;
      changed = false;
      countOptimized = false;
    }

    protected override IColumn InternalExecute()
    {
      return result;
    }

    protected override bool IsEquals(Signature signature)
    {
      if (GetType() != signature.GetType())
        return false;
      AggregateFunction aggregateFunction = (AggregateFunction) signature;
      if (expression == aggregateFunction.expression)
        return distinct == aggregateFunction.distinct;
      return false;
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
    }

    public override void SetChanged()
    {
      if (expression != null)
        expression.SetChanged();
      changed = false;
    }

    public override void ClearChanged()
    {
      if (expression != null)
        expression.ClearChanged();
      changed = false;
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      distinct = this.distinct;
      return true;
    }

    public override SignatureType OnPrepare()
    {
      if (expression != null)
        expression = ConstantSignature.PrepareAndCheckConstant(expression, VistaDBType.Unknown);
      return signatureType;
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
      if (!changed && !(expression == null))
        return expression.GetIsChanged();
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
        return distinct;
      }
    }

    public Signature Expression
    {
      get
      {
        return expression;
      }
    }

    public void CreateEmptyResult()
    {
      if (result == null)
        result = CreateColumn(dataType);
      val = InternalCreateEmptyResult();
            result.Value = val;
      changed = true;
    }

    public void CreateNewGroup(object newVal)
    {
      if (result == null)
        result = CreateColumn(dataType);
      val = InternalCreateNewGroup(newVal);
      changed = true;
    }

    public bool AddRowToGroup(object newVal)
    {
      val = InternalAddRowToGroup(newVal);
      return !countOptimized;
    }

    public void FinishGroup()
    {
      val = InternalFinishGroup();
            result.Value = val;
    }

    public void CreateNewGroupAndSerialize(object newVal, ref object serObj)
    {
      CreateNewGroup(newVal);
      InternalSerialize(ref serObj);
    }

    public void AddRowToGroupAndSerialize(object newVal, ref object serObj)
    {
      InternalDeserialize(serObj);
      AddRowToGroup(newVal);
      InternalSerialize(ref serObj);
    }

    public void FinishGroup(object serObj)
    {
      InternalDeserialize(serObj);
      FinishGroup();
    }

    protected abstract void InternalSerialize(ref object serObj);

    protected abstract void InternalDeserialize(object serObj);

    protected abstract object InternalCreateEmptyResult();

    protected abstract object InternalCreateNewGroup(object newVal);

    protected abstract object InternalAddRowToGroup(object newVal);

    protected abstract object InternalFinishGroup();
  }
}
