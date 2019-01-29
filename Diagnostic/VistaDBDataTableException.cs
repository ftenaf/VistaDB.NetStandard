using System;
using System.Runtime.Serialization;

namespace VistaDB.Diagnostic
{
  [Serializable]
  public class VistaDBDataTableException : VistaDBException
  {
    public VistaDBDataTableException(int errorId)
      : base(errorId)
    {
    }

    public VistaDBDataTableException(Exception e, int errorId)
      : base(e, errorId)
    {
    }

    protected VistaDBDataTableException(SerializationInfo info, StreamingContext context)
      : base(info, context)
    {
    }

    public bool IsCritical
    {
      get
      {
        return this.Contains(140L);
      }
    }
  }
}
