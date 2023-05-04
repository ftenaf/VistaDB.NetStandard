using System.Collections.Generic;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class BetweenOperator : Signature
  {
    protected Signature expression;
    protected Signature beginExpression;
    protected Signature endExpression;
    private IColumn beginValue;
    private IColumn endValue;
    private IColumn expValue1;
    private IColumn expValue2;

    public BetweenOperator(Signature expression, SQLParser parser)
      : base(parser)
    {
      signatureType = SignatureType.Expression;
      dataType = VistaDBType.Bit;
      this.expression = expression;
      beginExpression = parser.NextSignature(true, true, 2);
      parser.ExpectedExpression("AND");
      endExpression = parser.NextSignature(true, true, 2);
      beginValue = (IColumn) null;
      endValue = (IColumn) null;
    }

    public override SignatureType OnPrepare()
    {
      expression = ConstantSignature.PrepareAndCheckConstant(expression, VistaDBType.Unknown);
      beginExpression = ConstantSignature.PrepareAndCheckConstant(beginExpression, VistaDBType.Unknown);
      endExpression = ConstantSignature.PrepareAndCheckConstant(endExpression, VistaDBType.Unknown);
      VistaDBType maxDataType1 = Utils.GetMaxDataType(expression.DataType, beginExpression.DataType);
      VistaDBType maxDataType2 = Utils.GetMaxDataType(expression.DataType, endExpression.DataType);
      if (!Utils.CompatibleTypes(expression.DataType, maxDataType1) || !Utils.CompatibleTypes(expression.DataType, maxDataType2) || (!Utils.CompatibleTypes(beginExpression.DataType, maxDataType1) || !Utils.CompatibleTypes(endExpression.DataType, maxDataType2)))
        throw new VistaDBSQLException(558, "BETWEEN", lineNo, symbolNo);
      CalcOptimizeLevel();
      beginValue = CreateColumn(maxDataType1);
      endValue = CreateColumn(maxDataType2);
      expValue1 = CreateColumn(maxDataType1);
      expValue2 = maxDataType1 == maxDataType2 ? expValue1 : CreateColumn(maxDataType2);
      if ((expression.SignatureType == SignatureType.Constant || expression.AlwaysNull) && (beginExpression.SignatureType == SignatureType.Constant || beginExpression.AlwaysNull) && (endExpression.SignatureType == SignatureType.Constant || endExpression.AlwaysNull))
        return SignatureType.Constant;
      return signatureType;
    }

    private void CalcOptimizeLevel()
    {
      if (!expression.Optimizable || !beginExpression.Optimizable || !endExpression.Optimizable)
      {
        optimizable = false;
      }
      else
      {
        switch (expression.SignatureType)
        {
          case SignatureType.Constant:
          case SignatureType.Column:
          case SignatureType.Parameter:
          case SignatureType.ExternalColumn:
            switch (beginExpression.SignatureType)
            {
              case SignatureType.Constant:
              case SignatureType.Column:
              case SignatureType.Parameter:
              case SignatureType.ExternalColumn:
                switch (endExpression.SignatureType)
                {
                  case SignatureType.Constant:
                  case SignatureType.Column:
                  case SignatureType.Parameter:
                  case SignatureType.ExternalColumn:
                    optimizable = true;
                    return;
                  default:
                    optimizable = false;
                    return;
                }
              default:
                optimizable = false;
                return;
            }
          default:
            optimizable = false;
            break;
        }
      }
    }

    protected override bool OnOptimize(ConstraintOperations constrainOperations)
    {
      if (expression.SignatureType == SignatureType.Column)
        return constrainOperations.AddLogicalBetween((ColumnSignature) expression, beginExpression, endExpression, false);
      return false;
    }

    protected override IColumn InternalExecute()
    {
      if (GetIsChanged())
      {
        IColumn column1 = expression.Execute();
        IColumn column2 = beginExpression.Execute();
        IColumn column3 = endExpression.Execute();
        bool flag = !column1.IsNull && !column2.IsNull && !column3.IsNull;
        if (flag)
        {
          Convert((IValue) column2, (IValue) beginValue);
          Convert((IValue) column3, (IValue) endValue);
          Convert((IValue) column1, (IValue) expValue1);
          if (!ReferenceEquals((object) expValue1, (object) expValue2))
            Convert((IValue) column1, (IValue) expValue2);
          flag = ProcessResult(expValue1.Compare((IVistaDBColumn) beginValue) >= 0 && expValue2.Compare((IVistaDBColumn) endValue) <= 0);
        }
        ((IValue) result).Value = (object) flag;
      }
      return result;
    }

    protected virtual bool ProcessResult(bool result)
    {
      return result;
    }

    protected override bool IsEquals(Signature signature)
    {
      if (signature.GetType() != GetType())
        return false;
      BetweenOperator betweenOperator = (BetweenOperator) signature;
      if (expression == betweenOperator.expression && beginExpression == betweenOperator.beginExpression)
        return endExpression == betweenOperator.endExpression;
      return false;
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
      expression = expression.Relink(signature, ref columnCount);
      beginExpression = beginExpression.Relink(signature, ref columnCount);
      endExpression = endExpression.Relink(signature, ref columnCount);
    }

    public override void SetChanged()
    {
      expression.SetChanged();
      beginExpression.SetChanged();
      endExpression.SetChanged();
    }

    public override void ClearChanged()
    {
      expression.ClearChanged();
      beginExpression.ClearChanged();
      endExpression.ClearChanged();
    }

    protected override bool InternalGetIsChanged()
    {
      if (!expression.GetIsChanged() && !beginExpression.GetIsChanged())
        return endExpression.GetIsChanged();
      return true;
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
      expression.GetAggregateFunctions(list);
      beginExpression.GetAggregateFunctions(list);
      endExpression.GetAggregateFunctions(list);
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      bool distinct1;
      bool distinct2;
      bool distinct3;
      bool flag = expression.HasAggregateFunction(out distinct1) | beginExpression.HasAggregateFunction(out distinct2) | endExpression.HasAggregateFunction(out distinct3);
      distinct = distinct1 || distinct2 || distinct3;
      return flag;
    }

    public override int ColumnCount
    {
      get
      {
        return expression.ColumnCount + beginExpression.ColumnCount + endExpression.ColumnCount;
      }
    }

    public override bool AlwaysNull
    {
      get
      {
        if (!expression.AlwaysNull && !beginExpression.AlwaysNull)
          return endExpression.AlwaysNull;
        return true;
      }
    }

    public override bool IsNull
    {
      get
      {
        if (!expression.IsNull && !beginExpression.IsNull)
          return endExpression.IsNull;
        return true;
      }
    }
  }
}
