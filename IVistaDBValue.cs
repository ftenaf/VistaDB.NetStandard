namespace VistaDB
{
  public interface IVistaDBValue
  {
    object Value { get; set; }

    bool IsNull { get; }

    VistaDBType Type { get; }

    System.Type SystemType { get; }
  }
}
