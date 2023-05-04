using System.Collections;
using System.Collections.Generic;
using VistaDB.DDA;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class ValueListSignature : Signature, IValueList, IEnumerable
  {
    private List<Signature> valueList;
    private IColumn tempValue;

    private ValueListSignature(SQLParser parser)
      : base(parser)
    {
      bool needSkip = false;
      valueList = new List<Signature>();
      do
      {
        valueList.Add(parser.NextSignature(needSkip, true, 6));
        needSkip = true;
      }
      while (parser.IsToken(","));
      signatureType = SignatureType.Expression;
      dataType = VistaDBType.Unknown;
      optimizable = false;
      tempValue = null;
    }

    internal static Signature CreateSignature(SQLParser parser)
    {
      return new ValueListSignature(parser);
    }

    public override SignatureType OnPrepare()
    {
      signatureType = SignatureType.Constant;
      foreach (Signature signature in valueList)
      {
        if (signature.Prepare() != SignatureType.Constant)
          signatureType = SignatureType.Expression;
      }
      dataType = valueList[0].DataType;
      return signatureType;
    }

    protected override IColumn InternalExecute()
    {
      return result;
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      distinct = false;
      return false;
    }

    protected override bool IsEquals(Signature signature)
    {
      if (!(signature is ValueListSignature))
        return false;
      ValueListSignature valueListSignature = (ValueListSignature) signature;
      if (valueListSignature.valueList.Count != valueList.Count)
        return false;
      for (int index = 0; index < valueList.Count; ++index)
      {
        if (valueListSignature.valueList[index] != valueList[index])
          return false;
      }
      return true;
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
      for (int index = 0; index < valueList.Count; ++index)
        valueList[index] = valueList[index].Relink(signature, ref columnCount);
    }

    public override void SetChanged()
    {
      for (int index = 0; index < valueList.Count; ++index)
        valueList[index].SetChanged();
    }

    public override void ClearChanged()
    {
      for (int index = 0; index < valueList.Count; ++index)
        valueList[index].ClearChanged();
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
      for (int index = 0; index < valueList.Count; ++index)
        valueList[index].GetAggregateFunctions(list);
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
      for (int index = 0; index < valueList.Count; ++index)
      {
        if (valueList[index].GetIsChanged())
          return true;
      }
      return false;
    }

    public override int ColumnCount
    {
      get
      {
        int num = 0;
        for (int index = 0; index < valueList.Count; ++index)
          num += valueList[index].ColumnCount;
        return num;
      }
    }

    public bool IsValuePresent(IColumn val)
    {
      if (tempValue == null)
        tempValue = CreateColumn(val.Type);
      for (int index = 0; index < valueList.Count; ++index)
      {
        Convert(valueList[index].Execute(), tempValue);
        if (Utils.IsCharacterDataType(val.Type) && !tempValue.IsNull)
                    tempValue.Value = ((string)tempValue.Value).TrimEnd();
        if (val.Compare(tempValue) == 0)
          return true;
      }
      return false;
    }

    internal int Count
    {
      get
      {
        return valueList.Count;
      }
    }

    public IEnumerator GetEnumerator()
    {
      return valueList.GetEnumerator();
    }
  }
}
