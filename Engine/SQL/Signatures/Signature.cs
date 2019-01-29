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
      this.text = token;
      this.parent = parent;
      this.dataType = VistaDBType.Unknown;
      this.result = (IColumn) null;
      this.isAllowNull = true;
      this.found = false;
      this.optimizable = false;
      this.tempRow = (SourceRow) null;
      this.tempColumnIndex = -1;
      this.signatureType = SignatureType.Constant;
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
      return ColumnsProperties.GetMaxLength(this.dataType);
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
        if (this.result != null)
          return this.result.IsNull;
        return false;
      }
    }

    public bool IsAllowNull
    {
      get
      {
        return this.isAllowNull;
      }
    }

    public VistaDBType DataType
    {
      get
      {
        return this.dataType;
      }
    }

    public SignatureType SignatureType
    {
      get
      {
        return this.signatureType;
      }
    }

    public IColumn Result
    {
      get
      {
        return this.result;
      }
    }

    public string Text
    {
      get
      {
        return this.text;
      }
    }

    public bool Optimizable
    {
      get
      {
        return this.optimizable;
      }
    }

    public Statement Parent
    {
      get
      {
        return this.parent;
      }
    }

    public int LineNo
    {
      get
      {
        return this.lineNo;
      }
    }

    public int SymbolNo
    {
      get
      {
        return this.symbolNo;
      }
    }

    public override int GetHashCode()
    {
      return base.GetHashCode();
    }

    public override bool Equals(object obj)
    {
      if ((object) (obj as Signature) != null)
        return this.IsEquals((Signature) obj);
      return false;
    }

    public static bool operator ==(Signature signature1, Signature signature2)
    {
      if (Signature.ObjIsNull((object) signature1))
        return Signature.ObjIsNull((object) signature2);
      return signature1.Equals((object) signature2);
    }

    public static bool operator !=(Signature signature1, Signature signature2)
    {
      if (Signature.ObjIsNull((object) signature1))
        return !Signature.ObjIsNull((object) signature2);
      return !signature1.Equals((object) signature2);
    }

    private static bool ObjIsNull(object obj)
    {
      return !(obj is Signature);
    }

    public Signature Relink(Signature signature, ref int columnCount)
    {
      if (this.found)
        return this;
      if (this == signature)
      {
        this.found = true;
        columnCount += this.ColumnCount;
        return signature;
      }
      this.RelinkParameters(signature, ref columnCount);
      return this;
    }

    public SignatureType Prepare()
    {
      return this.OnPrepare();
    }

    public IColumn Execute()
    {
      if (this.result == null && this.dataType != VistaDBType.Unknown)
        this.result = this.CreateColumn(this.dataType);
      if (this.tempRow == null)
        return this.InternalExecute();
      if (this.tempRow.Columns == null)
        ((IValue) this.result).Value = ((IValue) this.tempRow.Row[this.tempColumnIndex]).Value;
      else
        ((IValue) this.result).Value = ((IValue) this.tempRow.Columns[this.tempColumnIndex]).Value;
      return this.result;
    }

    public IColumn SimpleExecute()
    {
      if (this.result == null)
        this.result = this.CreateColumn(this.dataType);
      this.OnSimpleExecute();
      return this.result;
    }

    public void SwitchToTempTable(SourceRow sourceRow, int columnIndex)
    {
      this.tempRow = sourceRow;
      this.tempColumnIndex = columnIndex;
    }

    public virtual void SwitchToTempTable(SourceRow sourceRow, int columnIndex, SelectStatement.ResultColumn resultColumn)
    {
    }

    public void SwitchToTable()
    {
      this.tempRow = (SourceRow) null;
      this.tempColumnIndex = -1;
    }

    public bool GetIsChanged()
    {
      if (this.tempRow == null)
        return this.InternalGetIsChanged();
      return true;
    }

    public bool Optimize(ConstraintOperations constrainOperations)
    {
      if (this.optimizable)
        return this.OnOptimize(constrainOperations);
      return false;
    }

    public IColumn CreateColumn(VistaDBType dataType)
    {
      if (this.parent.Connection.Database == null)
        return (IColumn) DataStorage.CreateRowColumn(dataType, true, CultureInfo.InvariantCulture);
      if (Utils.IsCharacterDataType(dataType))
        return this.parent.Database.CreateEmtpyUnicodeColumn();
      return this.parent.Database.CreateEmptyColumn(dataType);
    }

    protected void Convert(IValue sourceValue, IValue destValue)
    {
      this.parent.Database.Conversion.Convert(sourceValue, destValue);
    }

    protected bool ExistConvertion(VistaDBType srcType, VistaDBType dstType)
    {
      return this.parent.Database.Conversion.ExistConvertion(srcType, dstType);
    }
  }
}
