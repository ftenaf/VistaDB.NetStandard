namespace VistaDB.DDA
{
  public interface IVistaDBOperationCallbackStatus
  {
    int Progress { get; }

    VistaDBOperationStatusTypes Operation { get; }

    string ObjectName { get; }

    string Message { get; }
  }
}
