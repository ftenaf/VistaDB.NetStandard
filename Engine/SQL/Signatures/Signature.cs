using System.Collections.Generic;
using System.Globalization;
using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal abstract class Signature
  {
    private bool found;
    private SourceRow tempRow;
    private int tempColumnIndex;
    protected SignatureType signatureType;
    protected VistaDBType dataType;
    protected int lineNo;
    protected int symbolNo;
    protected string text;
    protected Statement parent;
    protected IColumn result;
    protected bool isAllowNull;
    protected bool optimizable;

    private Signature(Statement parent, int lineNo, int symbolNo, string token)
    {
      this.lineNo = lineNo;
      this.symbolNo = symbolNo;
      text = token;
      this.parent = parent;
      dataType = VistaDBType.Unknown;
      result = (IColumn) null;
      isAllowNull = true;
      found = false;
      optimizable = false;
      tempRow = (SourceRow) null;
      tempColumnIndex = -1;
      signatureType = SignatureType.Constant;
    }

    protected Signature(Statement parent)
      : this(parent, 0, 0, string.Empty)
    {
    }

    protected Signature(SQLParser parser)
      : this(parser.Parent, parser.TokenValue.RowNo, parser.TokenValue.ColNo, parser.TokenValue.Token)
    {
    }

    protected abstract IColumn InternalExecute();

    public abstract bool HasAggregateFunction(out bool distinct);

    public abstract SignatureType OnPrepare();

    protected abstract bool IsEquals(Signature signature);

    protected abstract void RelinkParameters(Signature signature, ref int columnCount);

    public abstract void SetChanged();

    public abstract void ClearChanged();

    protected abstract bool InternalGetIsChanged();

    public abstract void GetAggregateFunctions(List<AggregateFunction> list);

    public virtual int GetWidth()
    {
      return ColumnsProperties.GetMaxLength(dataType);
    }

    protected virtual bool OnOptimize(ConstraintOperations constrainOperations)
    {
      return false;
    }

    protected virtual void OnSimpleExecute()
    {
    }

    public abstract bool AlwaysNull { get; }

    public abstract int ColumnCount { get; }

    public virtual bool IsNull
    {
      get
      {
        if (result != null)
          return result.IsNull;
        return false;
      }
    }

    public bool IsAllowNull
    {
      get
      {
        return isAllowNull;
      }
    }

    public VistaDBType DataType
    {
      get
      {
        return dataType;
      }
    }

    public SignatureType SignatureType
    {
      get
      {
        return signatureType;
      }
    }

    public IColumn Result
    {
      get
      {
        return result;
      }
    }

    public string Text
    {
      get
      {
        return text;
      }
    }

    public bool Optimizable
    {
      get
      {
        return optimizable;
      }
    }

    public Statement Parent
    {
      get
      {
        return parent;
      }
    }

    public int LineNo
    {
      get
      {
        return lineNo;
      }
    }

    public int SymbolNo
    {
      get
      {
        return symbolNo;
      }
    }

    public override int GetHashCode()
    {
      return base.GetHashCode();
    }

    public override bool Equals(object obj)
    {
      if ((object) (obj as Signature) != null)
        return IsEquals((Signature) obj);
      return false;
    }

    public static bool operator ==(Signature signature1, Signature signature2)
    {
      if (ObjIsNull((object) signature1))
        return ObjIsNull((object) signature2);
      return signature1.Equals((object) signature2);
    }

    public static bool operator !=(Signature signature1, Signature signature2)
    {
      if (ObjIsNull((object) signature1))
        return !ObjIsNull((object) signature2);
      return !signature1.Equals((object) signature2);
    }

    private static bool ObjIsNull(object obj)
    {
      return !(obj is Signature);
    }

    public Signature Relink(Signature signature, ref int columnCount)
    {
      if (found)
        return this;
      if (this == signature)
      {
        found = true;
        columnCount += ColumnCount;
        return signature;
      }
      RelinkParameters(signature, ref columnCount);
      return this;
    }

    public SignatureType Prepare()
    {
      return OnPrepare();
    }

    public IColumn Execute()
    {
      if (result == null && dataType != VistaDBType.Unknown)
        result = CreateColumn(dataType);
      if (tempRow == null)
        return InternalExecute();
      if (tempRow.Columns == null)
        ((IValue) result).Value = ((IValue) tempRow.Row[tempColumnIndex]).Value;
      else
        ((IValue) result).Value = ((IValue) tempRow.Columns[tempColumnIndex]).Value;
      return result;
    }

    public IColumn SimpleExecute()
    {
      if (result == null)
        result = CreateColumn(dataType);
      OnSimpleExecute();
      return result;
    }

    public void SwitchToTempTable(SourceRow sourceRow, int columnIndex)
    {
      tempRow = sourceRow;
      tempColumnIndex = columnIndex;
    }

    public virtual void SwitchToTempTable(SourceRow sourceRow, int columnIndex, SelectStatement.ResultColumn resultColumn)
    {
    }

    public void SwitchToTable()
    {
      tempRow = (SourceRow) null;
      tempColumnIndex = -1;
    }

    public bool GetIsChanged()
    {
      if (tempRow == null)
        return InternalGetIsChanged();
      return true;
    }

    public bool Optimize(ConstraintOperations constrainOperations)
    {
      if (optimizable)
        return OnOptimize(constrainOperations);
      return false;
    }

    public IColumn CreateColumn(VistaDBType dataType)
    {
      if (parent.Connection.Database == null)
        return (IColumn) DataStorage.CreateRowColumn(dataType, true, CultureInfo.InvariantCulture);
      if (Utils.IsCharacterDataType(dataType))
        return parent.Database.CreateEmtpyUnicodeColumn();
      return parent.Database.CreateEmptyColumn(dataType);
    }

    protected void Convert(IValue sourceValue, IValue destValue)
    {
      parent.Database.Conversion.Convert(sourceValue, destValue);
    }

    protected bool ExistConvertion(VistaDBType srcType, VistaDBType dstType)
    {
      return parent.Database.Conversion.ExistConvertion(srcType, dstType);
    }
  }
}
