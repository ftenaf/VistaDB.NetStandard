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
      this.inputExpression = parser.IsToken("WHEN") ? (Signature) null : parser.NextSignature(false, true, 6);
      this.whenExpressions = new List<Signature>();
      this.thenExpressions = new List<Signature>();
      while (parser.IsToken("WHEN"))
      {
        Signature signature = parser.NextSignature(true, true, 6);
        if (this.inputExpression != (Signature) null && !(signature is ConstantSignature) && !(signature is UnaryMinusOperator))
          throw new VistaDBSQLException(635, "", signature.LineNo, signature.SymbolNo);
        this.whenExpressions.Add(signature);
        parser.ExpectedExpression("THEN");
        this.thenExpressions.Add(parser.NextSignature(true, true, 6));
      }
      if (this.whenExpressions.Count == 0)
        throw new VistaDBSQLException(581, "", this.lineNo, this.symbolNo);
      this.elseExpression = !parser.IsToken("ELSE") ? (Signature) null : parser.NextSignature(true, true, 6);
      parser.ExpectedExpression("END");
      this.signatureType = SignatureType.Expression;
      this.conditionType = VistaDBType.Unknown;
      this.tempValue = (IColumn) null;
      this.width = 0;
    }

    protected override bool IsEquals(Signature signature)
    {
      if (this.GetType() != signature.GetType())
        return false;
      CaseFunction caseFunction = (CaseFunction) signature;
      if (this.whenExpressions.Count != caseFunction.whenExpressions.Count || this.elseExpression != caseFunction.elseExpression || this.inputExpression != caseFunction.inputExpression)
        return false;
      for (int index = 0; index < this.whenExpressions.Count; ++index)
      {
        if (this.whenExpressions[index] != caseFunction.whenExpressions[index] || this.thenExpressions[index] != caseFunction.thenExpressions[index])
          return false;
      }
      return true;
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
      if (this.inputExpression != (Signature) null)
        this.inputExpression = this.inputExpression.Relink(signature, ref columnCount);
      if (this.elseExpression != (Signature) null)
        this.elseExpression = this.elseExpression.Relink(signature, ref columnCount);
      for (int index = 0; index < this.whenExpressions.Count; ++index)
      {
        this.whenExpressions[index].Relink(this.whenExpressions[index], ref columnCount);
        this.thenExpressions[index].Relink(this.thenExpressions[index], ref columnCount);
      }
    }

    public override void SetChanged()
    {
      if (this.inputExpression != (Signature) null)
        this.inputExpression.SetChanged();
      if (this.elseExpression != (Signature) null)
        this.elseExpression.SetChanged();
      for (int index = 0; index < this.whenExpressions.Count; ++index)
      {
        this.whenExpressions[index].SetChanged();
        this.thenExpressions[index].SetChanged();
      }
    }

    public override void ClearChanged()
    {
      if (this.inputExpression != (Signature) null)
        this.inputExpression.ClearChanged();
      if (this.elseExpression != (Signature) null)
        this.elseExpression.ClearChanged();
      for (int index = 0; index < this.whenExpressions.Count; ++index)
      {
        this.whenExpressions[index].ClearChanged();
        this.thenExpressions[index].ClearChanged();
      }
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      bool flag = false;
      distinct = false;
      if (this.inputExpression != (Signature) null && this.inputExpression.HasAggregateFunction(out distinct))
      {
        if (distinct)
          return true;
        flag = true;
      }
      if (this.elseExpression != (Signature) null && this.elseExpression.HasAggregateFunction(out distinct))
      {
        if (distinct)
          return true;
        flag = true;
      }
      for (int index = 0; index < this.whenExpressions.Count; ++index)
      {
        if (this.whenExpressions[index].HasAggregateFunction(out distinct))
        {
          if (distinct)
            return true;
          flag = true;
        }
        if (this.thenExpressions[index].HasAggregateFunction(out distinct))
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
      if (this.inputExpression != (Signature) null)
      {
        this.inputExpression = ConstantSignature.PrepareAndCheckConstant(this.inputExpression, VistaDBType.Unknown);
        this.conditionType = this.inputExpression.DataType;
        if (this.inputExpression.SignatureType != SignatureType.Constant)
          signatureType = SignatureType.Expression;
      }
      else
        this.conditionType = VistaDBType.Bit;
      if (signatureType == SignatureType.Constant)
      {
        signatureType = this.PrepareBody();
      }
      else
      {
        int num = (int) this.PrepareBody();
      }
      this.tempValue = this.CreateColumn(this.conditionType);
      if (signatureType != SignatureType.Constant && !this.AlwaysNull)
        return this.signatureType;
      return SignatureType.Constant;
    }

    protected override IColumn InternalExecute()
    {
      if (this.GetIsChanged())
      {
        bool flag = false;
        for (int index = 0; index < this.whenExpressions.Count; ++index)
        {
          this.Convert((IValue) this.whenExpressions[index].Execute(), (IValue) this.tempValue);
          if (this.inputExpression == (Signature) null)
          {
            if (this.tempValue.IsNull || !(bool) ((IValue) this.tempValue).Value)
              continue;
          }
          else
          {
            this.inputExpression.Execute();
            if (this.inputExpression.Result.Compare((IVistaDBColumn) this.tempValue) != 0)
              continue;
          }
          this.Convert((IValue) this.thenExpressions[index].Execute(), (IValue) this.result);
          flag = true;
          break;
        }
        if (!flag && this.elseExpression != (Signature) null)
        {
          this.Convert((IValue) this.elseExpression.Execute(), (IValue) this.result);
          flag = true;
        }
        if (!flag)
          ((IValue) this.result).Value = (object) null;
      }
      return this.result;
    }

    public override bool AlwaysNull
    {
      get
      {
        if (this.inputExpression != (Signature) null && this.inputExpression.AlwaysNull)
          return true;
        if (!(this.elseExpression == (Signature) null) && !this.elseExpression.AlwaysNull)
          return false;
        for (int index = 0; index < this.whenExpressions.Count; ++index)
        {
          if (!this.whenExpressions[index].AlwaysNull && !this.thenExpressions[index].AlwaysNull)
            return false;
        }
        return true;
      }
    }

    protected override bool InternalGetIsChanged()
    {
      bool flag = this.inputExpression != (Signature) null && this.inputExpression.GetIsChanged() || this.elseExpression != (Signature) null && this.elseExpression.GetIsChanged();
      int index = 0;
      for (int count = this.whenExpressions.Count; !flag && index < count; ++index)
        flag = this.whenExpressions[index].GetIsChanged() || this.thenExpressions[index].GetIsChanged();
      return flag;
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
      if (this.inputExpression != (Signature) null)
        this.inputExpression.GetAggregateFunctions(list);
      if (this.elseExpression != (Signature) null)
        this.elseExpression.GetAggregateFunctions(list);
      for (int index = 0; index < this.whenExpressions.Count; ++index)
      {
        this.whenExpressions[index].GetAggregateFunctions(list);
        this.thenExpressions[index].GetAggregateFunctions(list);
      }
    }

    public override int GetWidth()
    {
      return this.width;
    }

    public override int ColumnCount
    {
      get
      {
        int num = 0;
        if (this.inputExpression != (Signature) null)
          num += this.inputExpression.ColumnCount;
        if (this.elseExpression != (Signature) null)
          num += this.elseExpression.ColumnCount;
        for (int index = 0; index < this.whenExpressions.Count; ++index)
          num = num + this.whenExpressions[index].ColumnCount + this.thenExpressions[index].ColumnCount;
        return num;
      }
    }

    private SignatureType PrepareBody()
    {
      int count = this.whenExpressions.Count;
      SignatureType signatureType1 = SignatureType.Constant;
      SignatureType signatureType2 = SignatureType.Constant;
      SignatureType[] signatureTypeArray = new SignatureType[count];
      for (int index = 0; index < count; ++index)
      {
        if (this.PrepareWhenExpression(index) != SignatureType.Constant)
          signatureType2 = SignatureType.Expression;
        signatureTypeArray[index] = this.PrepareThenExpression(index);
      }
      if (this.elseExpression != (Signature) null)
      {
        signatureType1 = this.elseExpression.Prepare();
        this.CalcMaxDataType(this.elseExpression);
      }
      for (int index = 0; index < count; ++index)
      {
        Signature signature = this.TryConvertToConst(this.thenExpressions[index], signatureTypeArray[index]);
        if (signature.SignatureType != SignatureType.Constant)
          signatureType2 = SignatureType.Expression;
        this.thenExpressions[index] = signature;
      }
      if (this.elseExpression != (Signature) null)
      {
        this.elseExpression = this.TryConvertToConst(this.elseExpression, signatureType1);
        if (this.elseExpression.SignatureType != SignatureType.Constant)
          signatureType2 = SignatureType.Expression;
      }
      return signatureType2;
    }

    private SignatureType PrepareWhenExpression(int index)
    {
      Signature signature = ConstantSignature.PrepareAndCheckConstant(this.whenExpressions[index], this.conditionType);
      this.whenExpressions[index] = signature;
      if (Utils.CompatibleTypes(signature.DataType, this.conditionType))
        return signature.SignatureType;
      if (this.inputExpression == (Signature) null)
        throw new VistaDBSQLException(582, "", this.lineNo, this.symbolNo);
      throw new VistaDBSQLException(583, "", this.lineNo, this.symbolNo);
    }

    private SignatureType PrepareThenExpression(int index)
    {
      Signature thenExpression = this.thenExpressions[index];
      SignatureType signatureType = thenExpression.Prepare();
      this.thenExpressions[index] = thenExpression;
      this.CalcMaxDataType(thenExpression);
      return signatureType;
    }

    private void CalcMaxDataType(Signature expr)
    {
      if (Utils.CompareRank(this.dataType, expr.DataType) >= 0)
        return;
      this.dataType = expr.DataType;
      int num = expr.GetWidth();
      if (Utils.IsCharacterDataType(this.dataType))
      {
        if (num == 0)
          num = 30;
        if (this.width >= num)
          return;
        this.width = num;
      }
      else
        this.width = num;
    }

    private Signature TryConvertToConst(Signature expr, SignatureType signatureType)
    {
      if (signatureType == SignatureType.Constant && (expr.SignatureType != SignatureType.Constant || expr.DataType != this.dataType))
        return (Signature) ConstantSignature.CreateSignature(expr.Execute(), this.dataType, expr.Parent);
      return expr;
    }

    private VistaDBType GetMaxType(VistaDBType type1, VistaDBType type2)
    {
      if (type1 == VistaDBType.Unknown)
        return type2;
      if (!Utils.CompatibleTypes(type1, type2))
        throw new VistaDBSQLException(584, "", this.lineNo, this.symbolNo);
      return Utils.GetMaxDataType(type1, type2);
    }
  }
}
