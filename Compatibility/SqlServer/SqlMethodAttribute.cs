namespace VistaDB.Compatibility.SqlServer
{
  public sealed class SqlMethodAttribute : SqlFunctionAttribute
  {
    public SqlMethodAttribute()
    {
      OnNullCall = true;
      IsMutator = false;
      InvokeIfReceiverIsNull = false;
    }

    public bool InvokeIfReceiverIsNull { get; set; }

    public bool IsMutator { get; set; }

    public bool OnNullCall { get; set; }
  }
}
