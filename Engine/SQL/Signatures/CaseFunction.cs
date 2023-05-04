using System.Collections.Generic;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class CaseFunction : Signature
  {
    private Signature inputExpression;
    private List<Signature> whenExpressions;
    private List<Signature> thenExpressions;
    private Signature elseExpression;
    private VistaDBType conditionType;
    private IColumn tempValue;
    private int width;

    public CaseFunction(SQLParser parser)
      : base(parser)
    {
      parser.SkipToken(true);
      inputExpression = parser.IsToken("WHEN") ? (Signature) null : parser.NextSignature(false, true, 6);
      whenExpressions = new List<Signature>();
      thenExpressions = new List<Signature>();
      while (parser.IsToken("WHEN"))
      {
        Signature signature = parser.NextSignature(true, true, 6);
        if (inputExpression != (Signature) null && !(signature is ConstantSignature) && !(signature is UnaryMinusOperator))
          throw new VistaDBSQLException(635, "", signature.LineNo, signature.SymbolNo);
        whenExpressions.Add(signature);
        parser.ExpectedExpression("THEN");
        thenExpressions.Add(parser.NextSignature(true, true, 6));
      }
      if (whenExpressions.Count == 0)
        throw new VistaDBSQLException(581, "", lineNo, symbolNo);
      elseExpression = !parser.IsToken("ELSE") ? (Signature) null : parser.NextSignature(true, true, 6);
      parser.ExpectedExpression("END");
      signatureType = SignatureType.Expression;
      conditionType = VistaDBType.Unknown;
      tempValue = (IColumn) null;
      width = 0;
    }

    protected override bool IsEquals(Signature signature)
    {
      if (GetType() != signature.GetType())
        return false;
      CaseFunction caseFunction = (CaseFunction) signature;
      if (whenExpressions.Count != caseFunction.whenExpressions.Count || elseExpression != caseFunction.elseExpression || inputExpression != caseFunction.inputExpression)
        return false;
      for (int index = 0; index < whenExpressions.Count; ++index)
      {
        if (whenExpressions[index] != caseFunction.whenExpressions[index] || thenExpressions[index] != caseFunction.thenExpressions[index])
          return false;
      }
      return true;
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
      if (inputExpression != (Signature) null)
        inputExpression = inputExpression.Relink(signature, ref columnCount);
      if (elseExpression != (Signature) null)
        elseExpression = elseExpression.Relink(signature, ref columnCount);
      for (int index = 0; index < whenExpressions.Count; ++index)
      {
        whenExpressions[index].Relink(whenExpressions[index], ref columnCount);
        thenExpressions[index].Relink(thenExpressions[index], ref columnCount);
      }
    }

    public override void SetChanged()
    {
      if (inputExpression != (Signature) null)
        inputExpression.SetChanged();
      if (elseExpression != (Signature) null)
        elseExpression.SetChanged();
      for (int index = 0; index < whenExpressions.Count; ++index)
      {
        whenExpressions[index].SetChanged();
        thenExpressions[index].SetChanged();
      }
    }

    public override void ClearChanged()
    {
      if (inputExpression != (Signature) null)
        inputExpression.ClearChanged();
      if (elseExpression != (Signature) null)
        elseExpression.ClearChanged();
      for (int index = 0; index < whenExpressions.Count; ++index)
      {
        whenExpressions[index].ClearChanged();
        thenExpressions[index].ClearChanged();
      }
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      bool flag = false;
      distinct = false;
      if (inputExpression != (Signature) null && inputExpression.HasAggregateFunction(out distinct))
      {
        if (distinct)
          return true;
        flag = true;
      }
      if (elseExpression != (Signature) null && elseExpression.HasAggregateFunction(out distinct))
      {
        if (distinct)
          return true;
        flag = true;
      }
      for (int index = 0; index < whenExpressions.Count; ++index)
      {
        if (whenExpressions[index].HasAggregateFunction(out distinct))
        {
          if (distinct)
            return true;
          flag = true;
        }
        if (thenExpressions[index].HasAggregateFunction(out distinct))
        {
          if (distinct)
            return true;
          flag = true;
        }
      }
      return flag;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = SignatureType.Constant;
      if (inputExpression != (Signature) null)
      {
        inputExpression = ConstantSignature.PrepareAndCheckConstant(inputExpression, VistaDBType.Unknown);
        conditionType = inputExpression.DataType;
        if (inputExpression.SignatureType != SignatureType.Constant)
          signatureType = SignatureType.Expression;
      }
      else
        conditionType = VistaDBType.Bit;
      if (signatureType == SignatureType.Constant)
      {
        signatureType = PrepareBody();
      }
      else
      {
        int num = (int) PrepareBody();
      }
      tempValue = CreateColumn(conditionType);
      if (signatureType != SignatureType.Constant && !AlwaysNull)
        return this.signatureType;
      return SignatureType.Constant;
    }

    protected override IColumn InternalExecute()
    {
      if (GetIsChanged())
      {
        bool flag = false;
        for (int index = 0; index < whenExpressions.Count; ++index)
        {
          Convert((IValue) whenExpressions[index].Execute(), (IValue) tempValue);
          if (inputExpression == (Signature) null)
          {
            if (tempValue.IsNull || !(bool) ((IValue) tempValue).Value)
              continue;
          }
          else
          {
            inputExpression.Execute();
            if (inputExpression.Result.Compare((IVistaDBColumn) tempValue) != 0)
              continue;
          }
          Convert((IValue) thenExpressions[index].Execute(), (IValue) result);
          flag = true;
          break;
        }
        if (!flag && elseExpression != (Signature) null)
        {
          Convert((IValue) elseExpression.Execute(), (IValue) result);
          flag = true;
        }
        if (!flag)
          ((IValue) result).Value = (object) null;
      }
      return result;
    }

    public override bool AlwaysNull
    {
      get
      {
        if (inputExpression != (Signature) null && inputExpression.AlwaysNull)
          return true;
        if (!(elseExpression == (Signature) null) && !elseExpression.AlwaysNull)
          return false;
        for (int index = 0; index < whenExpressions.Count; ++index)
        {
          if (!whenExpressions[index].AlwaysNull && !thenExpressions[index].AlwaysNull)
            return false;
        }
        return true;
      }
    }

    protected override bool InternalGetIsChanged()
    {
      bool flag = inputExpression != (Signature) null && inputExpression.GetIsChanged() || elseExpression != (Signature) null && elseExpression.GetIsChanged();
      int index = 0;
      for (int count = whenExpressions.Count; !flag && index < count; ++index)
        flag = whenExpressions[index].GetIsChanged() || thenExpressions[index].GetIsChanged();
      return flag;
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
      if (inputExpression != (Signature) null)
        inputExpression.GetAggregateFunctions(list);
      if (elseExpression != (Signature) null)
        elseExpression.GetAggregateFunctions(list);
      for (int index = 0; index < whenExpressions.Count; ++index)
      {
        whenExpressions[index].GetAggregateFunctions(list);
        thenExpressions[index].GetAggregateFunctions(list);
      }
    }

    public override int GetWidth()
    {
      return width;
    }

    public override int ColumnCount
    {
      get
      {
        int num = 0;
        if (inputExpression != (Signature) null)
          num += inputExpression.ColumnCount;
        if (elseExpression != (Signature) null)
          num += elseExpression.ColumnCount;
        for (int index = 0; index < whenExpressions.Count; ++index)
          num = num + whenExpressions[index].ColumnCount + thenExpressions[index].ColumnCount;
        return num;
      }
    }

    private SignatureType PrepareBody()
    {
      int count = whenExpressions.Count;
      SignatureType signatureType1 = SignatureType.Constant;
      SignatureType signatureType2 = SignatureType.Constant;
      SignatureType[] signatureTypeArray = new SignatureType[count];
      for (int index = 0; index < count; ++index)
      {
        if (PrepareWhenExpression(index) != SignatureType.Constant)
          signatureType2 = SignatureType.Expression;
        signatureTypeArray[index] = PrepareThenExpression(index);
      }
      if (elseExpression != (Signature) null)
      {
        signatureType1 = elseExpression.Prepare();
        CalcMaxDataType(elseExpression);
      }
      for (int index = 0; index < count; ++index)
      {
        Signature signature = TryConvertToConst(thenExpressions[index], signatureTypeArray[index]);
        if (signature.SignatureType != SignatureType.Constant)
          signatureType2 = SignatureType.Expression;
        thenExpressions[index] = signature;
      }
      if (elseExpression != (Signature) null)
      {
        elseExpression = TryConvertToConst(elseExpression, signatureType1);
        if (elseExpression.SignatureType != SignatureType.Constant)
          signatureType2 = SignatureType.Expression;
      }
      return signatureType2;
    }

    private SignatureType PrepareWhenExpression(int index)
    {
      Signature signature = ConstantSignature.PrepareAndCheckConstant(whenExpressions[index], conditionType);
      whenExpressions[index] = signature;
      if (Utils.CompatibleTypes(signature.DataType, conditionType))
        return signature.SignatureType;
      if (inputExpression == (Signature) null)
        throw new VistaDBSQLException(582, "", lineNo, symbolNo);
      throw new VistaDBSQLException(583, "", lineNo, symbolNo);
    }

    private SignatureType PrepareThenExpression(int index)
    {
      Signature thenExpression = thenExpressions[index];
      SignatureType signatureType = thenExpression.Prepare();
      thenExpressions[index] = thenExpression;
      CalcMaxDataType(thenExpression);
      return signatureType;
    }

    private void CalcMaxDataType(Signature expr)
    {
      if (Utils.CompareRank(dataType, expr.DataType) >= 0)
        return;
      dataType = expr.DataType;
      int num = expr.GetWidth();
      if (Utils.IsCharacterDataType(dataType))
      {
        if (num == 0)
          num = 30;
        if (width >= num)
          return;
        width = num;
      }
      else
        width = num;
    }

    private Signature TryConvertToConst(Signature expr, SignatureType signatureType)
    {
      if (signatureType == SignatureType.Constant && (expr.SignatureType != SignatureType.Constant || expr.DataType != dataType))
        return (Signature) ConstantSignature.CreateSignature(expr.Execute(), dataType, expr.Parent);
      return expr;
    }
    }
}
