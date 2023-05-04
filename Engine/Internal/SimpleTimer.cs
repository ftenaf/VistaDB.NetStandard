using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace VistaDB.Engine.Internal
{
  public static class SimpleTimer
  {
    private static object s_SyncObject = new object();
    private static Dictionary<string, TimeCounter> s_Timers = new Dictionary<string, TimeCounter>();

    [Conditional("DEBUG")]
    public static void Start(string timerName)
    {
      lock (s_SyncObject)
      {
                TimeCounter timeCounter = FindTimer(timerName);
        if (timeCounter == null)
        {
          timeCounter = new TimeCounter(timerName);
                    s_Timers.Add(timerName, timeCounter);
        }
        if (!timeCounter.Stopwatch.IsRunning)
          timeCounter.Stopwatch.Start();
        ++timeCounter.Count;
      }
    }

    [Conditional("DEBUG")]
    public static void Stop(string timerName)
    {
      lock (s_SyncObject)
                FindTimer(timerName)?.Stopwatch.Stop();
    }

    [Conditional("DEBUG")]
    public static void WriteLine(string timerName)
    {
      lock (s_SyncObject)
      {
                TimeCounter timer = FindTimer(timerName);
        if (timer == null)
          return;
        timer.Stopwatch.Stop();
        string name = timer.Name;
        int count = timer.Count;
        double totalMilliseconds = timer.Stopwatch.Elapsed.TotalMilliseconds;
        if (count != 0)
        {
          double num = totalMilliseconds / (double) count;
        }
        timer.Reset();
      }
    }

    [Conditional("DEBUG")]
    public static void Reset(string timerName)
    {
      lock (s_SyncObject)
                FindTimer(timerName)?.Reset();
    }

    [Conditional("DEBUG")]
    public static void Write(StringBuilder output, string timerName, bool reset)
    {
      lock (s_SyncObject)
      {
                TimeCounter timer = FindTimer(timerName);
        if (timer == null)
        {
          output.AppendLine("Undefined timer: " + timerName);
        }
        else
        {
          string name = timer.Name;
          int count = timer.Count;
          double totalMilliseconds = timer.Stopwatch.Elapsed.TotalMilliseconds;
          double num = count == 0 ? 0.0 : totalMilliseconds / (double) count;
          output.AppendLine("Timer " + name + ": " + totalMilliseconds.ToString("F3") + " ms / " + (object) count + " calls = " + num.ToString("F3") + " ms / call");
          if (!reset)
            return;
          timer.Reset();
        }
      }
    }

    [Conditional("DEBUG")]
    public static void WriteAll(StringBuilder output, bool reset)
    {
    }

    [Conditional("DEBUG")]
    public static void WriteAll(StringBuilder output, bool reset, bool dropAll)
    {
      lock (s_SyncObject)
      {
        foreach (KeyValuePair<string, TimeCounter> timer in s_Timers)
        {
                    TimeCounter timeCounter = timer.Value;
          string name = timeCounter.Name;
          int count = timeCounter.Count;
          double totalMilliseconds = timeCounter.Stopwatch.Elapsed.TotalMilliseconds;
          double num = count == 0 ? 0.0 : totalMilliseconds / (double) count;
          output.AppendLine("Timer " + name + ": " + totalMilliseconds.ToString("F3") + " ms / " + (object) count + " calls = " + num.ToString("F3") + " ms / call");
          if (reset || dropAll)
          {
            timeCounter.Stopwatch.Stop();
            timeCounter.Reset();
          }
        }
        if (!dropAll)
          return;
                s_Timers.Clear();
      }
    }

    [Conditional("DEBUG")]
    public static void DropAll()
    {
      lock (s_SyncObject)
      {
        foreach (KeyValuePair<string, TimeCounter> timer in s_Timers)
          timer.Value?.Stopwatch.Stop();
                s_Timers.Clear();
      }
    }

    [Conditional("DEBUG")]
    public static void ReportStats(StringBuilder output)
    {
      lock (s_SyncObject)
      {
        Dictionary<string, int> dictionary = new Dictionary<string, int>(s_Timers.Count);
        foreach (KeyValuePair<string, TimeCounter> timer in s_Timers)
        {
                    TimeCounter timeCounter = timer.Value;
          string name = timeCounter.Name;
          int count = timeCounter.Count;
          timeCounter.Stopwatch.Stop();
          timeCounter.Reset();
          string str;
          int num1;
          int num2;
          if (name.EndsWith("_cacheSave"))
          {
            str = name.Remove(name.Length - "_cacheSave".Length);
            if (!dictionary.TryGetValue(str + "_cacheRead", out num1))
            {
              dictionary.Add(name, count);
              continue;
            }
            num2 = count;
          }
          else if (name.EndsWith("_cacheRead"))
          {
            str = name.Remove(name.Length - "_cacheRead".Length);
            if (!dictionary.TryGetValue(str + "_cacheSave", out num2))
            {
              dictionary.Add(name, count);
              continue;
            }
            num1 = count;
          }
          else
            continue;
          int num3 = num1 + num2;
          if (num3 > 0)
          {
            int num4 = 100 * num1 / num3;
            output.AppendFormat("Cache hit rate for:  {0}  {1}%", (object) str, (object) num4);
            output.AppendLine();
          }
        }
                s_Timers.Clear();
      }
    }

    private static TimeCounter FindTimer(string timerName)
    {
            TimeCounter timeCounter;
      if (!s_Timers.TryGetValue(timerName, out timeCounter))
        return (TimeCounter) null;
      return timeCounter;
    }

    private class TimeCounter
    {
      public string Name { get; private set; }

      public int Count { get; set; }

      public Stopwatch Stopwatch { get; private set; }

      public TimeCounter(string timerName)
      {
        Name = timerName;
        Count = 0;
        Stopwatch = new Stopwatch();
      }

      public void Reset()
      {
        Stopwatch.Reset();
        Count = 0;
      }
    }
  }
}
