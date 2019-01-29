using System;
using VistaDB.Diagnostic;

namespace VistaDB.Provider
{
  public sealed class VistaDBInfoMessageEventArgs : EventArgs
  {
    internal VistaDBInfoMessageEventArgs(string message, string source)
    {
      this.Message = message;
      this.Source = source;
    }

    internal VistaDBInfoMessageEventArgs(VistaDBException exception)
      : this(exception.Message, exception.Source)
    {
      this.Exception = exception;
    }

    private VistaDBException Exception { get; set; }

    public string Message { get; private set; }

    public string Source { get; private set; }
  }
}
