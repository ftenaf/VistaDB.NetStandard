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
      CopyFrom(unit);
    }

    internal PCodeUnit(Signature signature)
    {
      this.signature = signature;
    }

    internal Signature Signature
    {
      get
      {
        return signature;
      }
      set
      {
        signature = value;
      }
    }

    internal DataStorage ActiveStorage
    {
      get
      {
        return activeStorage;
      }
      set
      {
        activeStorage = value;
      }
    }

    internal int DelimitersCount
    {
      get
      {
        return delimiters;
      }
      set
      {
        delimiters = value;
      }
    }

    internal int ParametersCount
    {
      get
      {
        return parameters;
      }
      set
      {
        parameters = value;
      }
    }

    internal int Depth
    {
      get
      {
        return depth;
      }
      set
      {
        depth = value;
      }
    }

    internal int ContentBgn
    {
      get
      {
        return bgnOffset;
      }
      set
      {
        bgnOffset = value;
      }
    }

    internal int ContentEnd
    {
      get
      {
        return endOffset;
      }
      set
      {
        endOffset = value;
      }
    }

    internal Row ResultRow
    {
      get
      {
        return resultRow;
      }
      set
      {
        resultRow = value;
      }
    }

    internal Row.Column ResultColumn
    {
      get
      {
        return resultColumn;
      }
      set
      {
        resultColumn = value;
      }
    }

    internal void CopyFrom(PCodeUnit unit)
    {
      if (unit == this)
        return;
      signature = unit.signature;
      activeStorage = unit.activeStorage;
      resultRow = unit.resultRow;
      resultColumn = unit.resultColumn;
      delimiters = unit.delimiters;
      parameters = unit.parameters;
      depth = unit.depth;
      bgnOffset = unit.bgnOffset;
      endOffset = unit.endOffset;
    }

    public override string ToString()
    {
      return new string(signature.Name);
    }
  }
}
