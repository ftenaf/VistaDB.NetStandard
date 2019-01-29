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
        return this.kind;
      }
      set
      {
        this.kind = value;
      }
    }

    public string FillRow
    {
      get
      {
        return this.fillMethod;
      }
      set
      {
        this.fillMethod = value;
      }
    }
  }
}
