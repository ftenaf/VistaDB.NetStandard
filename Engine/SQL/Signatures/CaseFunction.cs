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
      inputExpression = parser.IsToken("WHEN") ? null : parser.NextSignature(false, true, 6);
      whenExpressions = new List<Signature>();
      thenExpressions = new List<Signature>();
      while (parser.IsToken("WHEN"))
      {
        Signature signature = parser.NextSignature(true, true, 6);
        if (inputExpression != null && !(signature is ConstantSignature) && !(signature is UnaryMinusOperator))
          throw new VistaDBSQLException(635, "", signature.LineNo, signature.SymbolNo);
        whenExpressions.Add(signature);
        parser.ExpectedExpression("THEN");
        thenExpressions.Add(parser.NextSignature(true, true, 6));
      }
      if (whenExpressions.Count == 0)
        throw new VistaDBSQLException(581, "", lineNo, symbolNo);
      elseExpression = !parser.IsToken("ELSE") ? null : parser.NextSignature(true, true, 6);
      parser.ExpectedExpression("END");
      signatureType = SignatureType.Expression;
      conditionType = VistaDBType.Unknown;
      tempValue = null;
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
      if (inputExpression != null)
        inputExpression = inputExpression.Relink(signature, ref columnCount);
      if (elseExpression != null)
        elseExpression = elseExpression.Relink(signature, ref columnCount);
      for (int index = 0; index < whenExpressions.Count; ++index)
      {
        whenExpressions[index].Relink(whenExpressions[index], ref columnCount);
        thenExpressions[index].Relink(thenExpressions[index], ref columnCount);
      }
    }

    public override void SetChanged()
    {
      if (inputExpression != null)
        inputExpression.SetChanged();
      if (elseExpression != null)
        elseExpression.SetChanged();
      for (int index = 0; index < whenExpressions.Count; ++index)
      {
        whenExpressions[index].SetChanged();
        thenExpressions[index].SetChanged();
      }
    }

    public override void ClearChanged()
    {
      if (inputExpression != null)
        inputExpression.ClearChanged();
      if (elseExpression != null)
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
      if (inputExpression != null && inputExpression.HasAggregateFunction(out distinct))
      {
        if (distinct)
          return true;
        flag = true;
      }
      if (elseExpression != null && elseExpression.HasAggregateFunction(out distinct))
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
      if (inputExpression != null)
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
          Convert(whenExpressions[index].Execute(), tempValue);
          if (inputExpression == null)
          {
            if (tempValue.IsNull || !(bool)tempValue.Value)
              continue;
          }
          else
          {
            inputExpression.Execute();
            if (inputExpression.Result.Compare(tempValue) != 0)
              continue;
          }
          Convert(thenExpressions[index].Execute(), result);
          flag = true;
          break;
        }
        if (!flag && elseExpression != null)
        {
          Convert(elseExpression.Execute(), result);
          flag = true;
        }
        if (!flag)
                    result.Value = null;
      }
      return result;
    }

    public override bool AlwaysNull
    {
      get
      {
        if (inputExpression != null && inputExpression.AlwaysNull)
          return true;
        if (!(elseExpression == null) && !elseExpression.AlwaysNull)
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
      bool flag = inputExpression != null && inputExpression.GetIsChanged() || elseExpression != null && elseExpression.GetIsChanged();
      int index = 0;
      for (int count = whenExpressions.Count; !flag && index < count; ++index)
        flag = whenExpressions[index].GetIsChanged() || thenExpressions[index].GetIsChanged();
      return flag;
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
      if (inputExpression != null)
        inputExpression.GetAggregateFunctions(list);
      if (elseExpression != null)
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
        if (inputExpression != null)
          num += inputExpression.ColumnCount;
        if (elseExpression != null)
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
      if (elseExpression != null)
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
      if (elseExpression != null)
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
      if (inputExpression == null)
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
        return ConstantSignature.CreateSignature(expr.Execute(), dataType, expr.Parent);
      return expr;
    }
    }
}
