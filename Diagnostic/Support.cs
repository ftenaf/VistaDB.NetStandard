namespace VistaDB.Diagnostic
{
  public sealed class Support
  {
    public static void BeginLogging()
    {
      Support.BeginLogging((string) null);
    }

    public static void BeginLogging(string message)
    {
    }

    public static void LogMessage(string message)
    {
    }

    public static void EndLogging()
    {
      Support.EndLogging((string) null);
    }

    public static void EndLogging(string message)
    {
    }

    public static bool IncludeCommandSQL
    {
      get
      {
        return false;
      }
      set
      {
      }
    }

    public static bool IncludeDataReaderRows
    {
      get
      {
        return false;
      }
      set
      {
      }
    }
  }
}
