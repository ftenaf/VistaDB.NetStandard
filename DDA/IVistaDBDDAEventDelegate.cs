namespace VistaDB.DDA
{
  public interface IVistaDBDDAEventDelegate
  {
    DDAEventDelegateType Type { get; }

    DDAEventDelegate EventDelegate { get; }

    object UsersData { get; }
  }
}
