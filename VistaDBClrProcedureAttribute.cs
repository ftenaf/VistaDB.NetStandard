using System;

namespace VistaDB
{
  [AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
  public sealed class VistaDBClrProcedureAttribute : Attribute
  {
    private VistaDBClrProcedureKind kind;
    private string fillMethod;

    public VistaDBClrProcedureKind Kind
    {
      get
      {
        return kind;
      }
      set
      {
        kind = value;
      }
    }

    public string FillRow
    {
      get
      {
        return fillMethod;
      }
      set
      {
        fillMethod = value;
      }
    }
  }
}
