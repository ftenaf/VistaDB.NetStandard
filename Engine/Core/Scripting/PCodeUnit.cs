namespace VistaDB.Engine.Core.Scripting
{
  internal class PCodeUnit
  {
    private Signature signature;
    private DataStorage activeStorage;
    private Row resultRow;
    private Row.Column resultColumn;
    private int delimiters;
    private int parameters;
    private int depth;
    private int bgnOffset;
    private int endOffset;

    internal PCodeUnit(PCodeUnit unit)
    {
      this.CopyFrom(unit);
    }

    internal PCodeUnit(Signature signature)
    {
      this.signature = signature;
    }

    internal Signature Signature
    {
      get
      {
        return this.signature;
      }
      set
      {
        this.signature = value;
      }
    }

    internal DataStorage ActiveStorage
    {
      get
      {
        return this.activeStorage;
      }
      set
      {
        this.activeStorage = value;
      }
    }

    internal int DelimitersCount
    {
      get
      {
        return this.delimiters;
      }
      set
      {
        this.delimiters = value;
      }
    }

    internal int ParametersCount
    {
      get
      {
        return this.parameters;
      }
      set
      {
        this.parameters = value;
      }
    }

    internal int Depth
    {
      get
      {
        return this.depth;
      }
      set
      {
        this.depth = value;
      }
    }

    internal int ContentBgn
    {
      get
      {
        return this.bgnOffset;
      }
      set
      {
        this.bgnOffset = value;
      }
    }

    internal int ContentEnd
    {
      get
      {
        return this.endOffset;
      }
      set
      {
        this.endOffset = value;
      }
    }

    internal Row ResultRow
    {
      get
      {
        return this.resultRow;
      }
      set
      {
        this.resultRow = value;
      }
    }

    internal Row.Column ResultColumn
    {
      get
      {
        return this.resultColumn;
      }
      set
      {
        this.resultColumn = value;
      }
    }

    internal void CopyFrom(PCodeUnit unit)
    {
      if (unit == this)
        return;
      this.signature = unit.signature;
      this.activeStorage = unit.activeStorage;
      this.resultRow = unit.resultRow;
      this.resultColumn = unit.resultColumn;
      this.delimiters = unit.delimiters;
      this.parameters = unit.parameters;
      this.depth = unit.depth;
      this.bgnOffset = unit.bgnOffset;
      this.endOffset = unit.endOffset;
    }

    public override string ToString()
    {
      return new string(this.signature.Name);
    }
  }
}
