using System;
using System.Runtime.Serialization;
using System.Security;

namespace VistaDB.Diagnostic
{
  [Serializable]
  public class VistaDBSQLException : VistaDBException
  {
    private int lineNo;
    private int symbolNo;

    internal VistaDBSQLException(int errorId, string hint, int lineNo, int symbolNo)
      : base(errorId, VistaDBSQLException.CreateMessage(hint, lineNo, symbolNo))
    {
      this.lineNo = lineNo;
      this.symbolNo = symbolNo;
    }

    internal VistaDBSQLException(Exception ex, int errorId, string hint, int lineNo, int symbolNo)
      : base(ex, errorId, VistaDBSQLException.CreateMessage(hint, lineNo, symbolNo))
    {
      this.lineNo = lineNo;
      this.symbolNo = symbolNo;
    }

    protected VistaDBSQLException(SerializationInfo info, StreamingContext context)
      : base(info, context)
    {
      this.lineNo = info.GetInt32("VistaDBLineNo");
      this.symbolNo = info.GetInt32("VistaDBSymbolNo");
    }

    [SecurityCritical]
    public override void GetObjectData(SerializationInfo info, StreamingContext context)
    {
      base.GetObjectData(info, context);
      info.AddValue("VistaDBLineNo", (object) this.lineNo, typeof (int));
      info.AddValue("VistaDBSymbolNo", (object) this.symbolNo, typeof (int));
    }

    private static string CreateMessage(string hint, int lineNo, int symbolNo)
    {
      return hint + "\r\nLine #: " + lineNo.ToString() + "; Column #: " + symbolNo.ToString();
    }

    public int LineNo
    {
      get
      {
        return this.lineNo;
      }
    }

    public int ColumnNo
    {
      get
      {
        return this.symbolNo;
      }
    }
  }
}
