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
      this.valueList = new List<Signature>();
      do
      {
        this.valueList.Add(parser.NextSignature(needSkip, true, 6));
        needSkip = true;
      }
      while (parser.IsToken(","));
      this.signatureType = SignatureType.Expression;
      this.dataType = VistaDBType.Unknown;
      this.optimizable = false;
      this.tempValue = (IColumn) null;
    }

    internal static Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new ValueListSignature(parser);
    }

    public override SignatureType OnPrepare()
    {
      this.signatureType = SignatureType.Constant;
      foreach (Signature signature in this.valueList)
      {
        if (signature.Prepare() != SignatureType.Constant)
          this.signatureType = SignatureType.Expression;
      }
      this.dataType = this.valueList[0].DataType;
      return this.signatureType;
    }

    protected override IColumn InternalExecute()
    {
      return this.result;
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
      if (valueListSignature.valueList.Count != this.valueList.Count)
        return false;
      for (int index = 0; index < this.valueList.Count; ++index)
      {
        if (valueListSignature.valueList[index] != this.valueList[index])
          return false;
      }
      return true;
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
      for (int index = 0; index < this.valueList.Count; ++index)
        this.valueList[index] = this.valueList[index].Relink(signature, ref columnCount);
    }

    public override void SetChanged()
    {
      for (int index = 0; index < this.valueList.Count; ++index)
        this.valueList[index].SetChanged();
    }

    public override void ClearChanged()
    {
      for (int index = 0; index < this.valueList.Count; ++index)
        this.valueList[index].ClearChanged();
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
      for (int index = 0; index < this.valueList.Count; ++index)
        this.valueList[index].GetAggregateFunctions(list);
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
      for (int index = 0; index < this.valueList.Count; ++index)
      {
        if (this.valueList[index].GetIsChanged())
          return true;
      }
      return false;
    }

    public override int ColumnCount
    {
      get
      {
        int num = 0;
        for (int index = 0; index < this.valueList.Count; ++index)
          num += this.valueList[index].ColumnCount;
        return num;
      }
    }

    public bool IsValuePresent(IColumn val)
    {
      if (this.tempValue == null)
        this.tempValue = this.CreateColumn(val.Type);
      for (int index = 0; index < this.valueList.Count; ++index)
      {
        this.Convert((IValue) this.valueList[index].Execute(), (IValue) this.tempValue);
        if (Utils.IsCharacterDataType(val.Type) && !this.tempValue.IsNull)
          ((IValue) this.tempValue).Value = (object) ((string) ((IValue) this.tempValue).Value).TrimEnd();
        if (val.Compare((IVistaDBColumn) this.tempValue) == 0)
          return true;
      }
      return false;
    }

    internal int Count
    {
      get
      {
        return this.valueList.Count;
      }
    }

    public IEnumerator GetEnumerator()
    {
      return (IEnumerator) this.valueList.GetEnumerator();
    }
  }
}
