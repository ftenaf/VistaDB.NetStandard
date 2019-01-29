using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Xml;

namespace VistaDB.Diagnostic
{
  internal class Logging : IDisposable
  {
    private static Logging Instance = (Logging) null;
    private static Exception LoadingError = (Exception) null;
    private static string _productName;
    private string _name;
    private bool _running;
    private Dictionary<Logging.LogMessageSeverity, Enum> _logMessageSeverity;
    private Dictionary<Logging.LogWriteMode, Enum> _logWriteMode;
    private Dictionary<Logging.SamplingType, Enum> _samplingType;
    private Dictionary<Logging.LogMethod, MethodInfo> _methods;
    private Dictionary<Logging.LogEvents, EventInfo> _events;

    internal static IDisposable QuickLog(string startupMessage, string shutdownMessage)
    {
      return (IDisposable) null;
    }

    [Conditional("LOG")]
    private static void SetupLogging(string message, EventHandler onInit)
    {
    }

    [Conditional("LOG")]
    internal static void Initialize()
    {
    }

    [Conditional("LOG")]
    internal static void Initialize(string message)
    {
    }

    [Conditional("LOG")]
    internal static void Initialize(string message, EventHandler onInit)
    {
    }

    [Conditional("LOG")]
    internal static void Shutdown()
    {
    }

    [Conditional("LOG")]
    internal static void Shutdown(string message)
    {
      if (Logging.Instance == null || !Logging.Instance._running)
        return;
      Logging.Instance.InternalShutdown(message);
    }

    internal static string GetProductName()
    {
      if (Logging._productName == null)
      {
        object[] customAttributes = typeof (Logging).Assembly.GetCustomAttributes(typeof (AssemblyProductAttribute), false);
        Logging._productName = customAttributes == null || customAttributes.Length <= 0 ? "Unknown" : ((AssemblyProductAttribute) customAttributes[0]).Product;
      }
      return Logging._productName;
    }

    [Conditional("LOG")]
    internal static void SetupConfiguration(EventArgs args)
    {
    }

    [Conditional("LOG")]
    internal static void SetupConfiguration(Logging.ReflectedProperties args)
    {
      Logging.ReflectedProperties reflectedProperties1 = args["Configuration"];
      Logging.ReflectedProperties reflectedProperties2 = reflectedProperties1["Publisher"];
      reflectedProperties2.SetValue<string>("ProductName", Logging.GetProductName());
      Assembly assembly = Assembly.GetEntryAssembly() ?? Assembly.GetExecutingAssembly();
      if (assembly != null)
      {
        object[] customAttributes1 = assembly.GetCustomAttributes(typeof (AssemblyTitleAttribute), false);
        if (customAttributes1 != null && customAttributes1.Length > 0)
          reflectedProperties2.SetValue<string>("ApplicationName", ((AssemblyTitleAttribute) customAttributes1[0]).Title);
        object[] customAttributes2 = assembly.GetCustomAttributes(typeof (AssemblyDescriptionAttribute), false);
        if (customAttributes2 != null && customAttributes2.Length > 0)
          reflectedProperties2.SetValue<string>("ApplicationDescription", ((AssemblyDescriptionAttribute) customAttributes2[0]).Description);
        object[] customAttributes3 = assembly.GetCustomAttributes(typeof (AssemblyVersionAttribute), false);
        if (customAttributes3 != null && customAttributes3.Length > 0)
          reflectedProperties2.SetValue<Version>("ApplicationVersion", new Version(((AssemblyVersionAttribute) customAttributes3[0]).Version));
        object[] customAttributes4 = assembly.GetCustomAttributes(typeof (AssemblyFileVersionAttribute), false);
        if (customAttributes4 != null && customAttributes4.Length > 0)
          reflectedProperties2.SetValue<Version>("ApplicationVersion", new Version(((AssemblyFileVersionAttribute) customAttributes4[0]).Version));
      }
      Logging.ReflectedProperties reflectedProperties3 = reflectedProperties1["Listener"];
      reflectedProperties3.SetValue<bool>("EnableNetworkEvents", false);
      reflectedProperties3.SetValue<bool>("EnableNetworkPerformance", false);
      reflectedProperties3.SetValue<bool>("EnableAssemblyEvents", false);
      reflectedProperties3.SetValue<bool>("EnableConsole", false);
      reflectedProperties3.SetValue<bool>("CatchApplicationExceptions", false);
      reflectedProperties3.SetValue<bool>("CatchUnhandledExceptions", false);
      reflectedProperties3.SetValue<bool>("ReportErrorsToUser", false);
      reflectedProperties3.SetValue<bool>("AutoTraceRegistration", false);
      reflectedProperties3.SetValue<bool>("EnableDiskPerformance", false);
      reflectedProperties3.SetValue<bool>("EnableProcessPerformance", false);
      reflectedProperties3.SetValue<bool>("EnablePowerEvents", false);
      reflectedProperties3.SetValue<bool>("EnableSystemPerformance", false);
      reflectedProperties3.SetValue<bool>("EnableUserEvents", false);
      Logging.ReflectedProperties reflectedProperties4 = reflectedProperties1["Viewer"];
      reflectedProperties4.SetValue<bool>("Enabled", false);
      reflectedProperties4.SetValue<string>("HotKey", string.Empty);
      reflectedProperties4.SetValue<int>("MaxMessages", Logging.Configuration.MaxViewerMessages);
      Logging.ReflectedProperties reflectedProperties5 = reflectedProperties1["SessionFile"];
      reflectedProperties5.SetValue<string>("Folder", Logging.Configuration.GetLogPath());
      reflectedProperties5.SetValue<bool>("EnableFilePruning", false);
    }

    [Conditional("LOG")]
    internal static void Counter(string name)
    {
    }

    [Conditional("LOG")]
    internal static void Counter(string name, int delta)
    {
      if (Logging.Instance == null || !Logging.Instance._running)
        return;
      Logging.Instance.InternalCounter(name, delta);
    }

    [Conditional("LOG")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static void Log(string name, string text)
    {
      if (Logging.Instance == null || !Logging.Instance._running)
        return;
      Logging.Instance.InternalWrite(Logging.LogMessageSeverity.Information, 1, (Exception) null, Logging.LogWriteMode.Queued, (string) null, name, text);
    }

    [Conditional("LOG")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static void Log(string name, string text, string details)
    {
      if (Logging.Instance == null || !Logging.Instance._running)
        return;
      Logging.Instance.InternalWrite(Logging.LogMessageSeverity.Information, 1, (Exception) null, Logging.LogWriteMode.Queued, details, name, text);
    }

    [Conditional("LOG")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static void Log(string name, Exception exception)
    {
      if (Logging.Instance == null || !Logging.Instance._running)
        return;
      Logging.Instance.InternalWrite(Logging.LogMessageSeverity.Error, 1, exception, Logging.LogWriteMode.Queued, (string) null, name, (string) null);
    }

    [Conditional("LOG")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static void Log(string name, Exception exception, string text)
    {
      if (Logging.Instance == null || !Logging.Instance._running)
        return;
      Logging.Instance.InternalWrite(Logging.LogMessageSeverity.Error, 1, exception, Logging.LogWriteMode.Queued, (string) null, name, text);
    }

    [Conditional("LOG")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static void Log(string name, string text, XmlDocument xml)
    {
      if (Logging.Instance == null || !Logging.Instance._running)
        return;
      Logging.Instance.InternalWrite(Logging.LogMessageSeverity.Information, 1, (Exception) null, Logging.LogWriteMode.Queued, xml.OuterXml, name, text);
    }

    [Conditional("LOG")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static void Log(string name, Exception exception, string text, XmlDocument xml)
    {
      if (Logging.Instance == null || !Logging.Instance._running)
        return;
      Logging.Instance.InternalWrite(Logging.LogMessageSeverity.Information, 1, exception, Logging.LogWriteMode.Queued, xml.OuterXml, name, text);
    }

    [Conditional("LOG")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static void Error(string name, string text)
    {
      if (Logging.Instance == null || !Logging.Instance._running)
        return;
      Logging.Instance.InternalWrite(Logging.LogMessageSeverity.Error, 1, (Exception) null, Logging.LogWriteMode.Queued, (string) null, name, text);
    }

    [Conditional("LOG")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static void Error(string name, Exception exception)
    {
      if (Logging.Instance == null || !Logging.Instance._running)
        return;
      Logging.Instance.InternalWrite(Logging.LogMessageSeverity.Critical, 1, exception, Logging.LogWriteMode.Queued, (string) null, name, (string) null);
    }

    [Conditional("LOG")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static void Error(string name, Exception exception, string text)
    {
      if (Logging.Instance == null || !Logging.Instance._running)
        return;
      Logging.Instance.InternalWrite(Logging.LogMessageSeverity.Critical, 1, exception, Logging.LogWriteMode.Queued, (string) null, name, text);
    }

    [Conditional("LOG")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static void Error(string name, string text, XmlDocument xml)
    {
      if (Logging.Instance == null || !Logging.Instance._running)
        return;
      Logging.Instance.InternalWrite(Logging.LogMessageSeverity.Error, 1, (Exception) null, Logging.LogWriteMode.Queued, xml.OuterXml, name, text);
    }

    [Conditional("LOG")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static void Assert(bool condition, string name, string message)
    {
      if (Logging.Instance == null || !Logging.Instance._running || condition)
        return;
      Logging.Instance.InternalWrite(Logging.LogMessageSeverity.Verbose, 1, (Exception) null, Logging.LogWriteMode.Queued, (string) null, name, message);
    }

    [Conditional("LOG")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static void Assert(bool condition, string name, Exception exception)
    {
      if (Logging.Instance == null || !Logging.Instance._running || condition && exception == null)
        return;
      Logging.Instance.InternalWrite(Logging.LogMessageSeverity.Verbose, 1, exception, Logging.LogWriteMode.Queued, (string) null, name, (string) null);
    }

    [Conditional("LOG")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static void Assert(bool condition, string name, Exception exception, string message)
    {
      if (Logging.Instance == null || !Logging.Instance._running || condition && exception == null)
        return;
      Logging.Instance.InternalWrite(Logging.LogMessageSeverity.Verbose, 1, exception, Logging.LogWriteMode.Queued, (string) null, name, message);
    }

    [Conditional("LOG")]
    internal static void CreateViewer(object parentControl)
    {
    }

    [Conditional("LOG")]
    internal static void CreateViewer(string name, object parentControl)
    {
      if (Logging.Instance == null)
        return;
      int num = Logging.Instance._running ? 1 : 0;
    }

    [Conditional("LOG")]
    private static void RemoveInitializeHandler(EventHandler handler)
    {
      if (Logging.Instance == null || !Logging.Instance._running)
        return;
      Logging.Instance.RemoveEvent(Logging.LogEvents.Initializing, handler);
    }

    private Logging(Type log, string startupMessage, EventHandler onInit)
    {
    }

    ~Logging()
    {
      this.Dispose(false);
    }

    private void AddEvent(Logging.LogEvents _event, EventHandler handler)
    {
      Delegate handler1 = Delegate.CreateDelegate(this._events[_event].EventHandlerType, handler.Target, handler.Method);
      this._events[_event].AddEventHandler((object) null, handler1);
    }

    private void RemoveEvent(Logging.LogEvents _event, EventHandler handler)
    {
      Delegate handler1 = Delegate.CreateDelegate(this._events[_event].EventHandlerType, handler.Target, handler.Method);
      this._events[_event].RemoveEventHandler((object) null, handler1);
    }

    private void Log_Initializing(object sender, EventArgs args)
    {
      Logging.ReflectedProperties reflectedProperties = new Logging.ReflectedProperties(args);
      AppDomain.CurrentDomain.DomainUnload += new EventHandler(this.CurrentDomain_DomainUnload);
      AppDomain.CurrentDomain.ProcessExit += new EventHandler(this.CurrentDomain_ProcessExit);
    }

    private void CurrentDomain_ProcessExit(object sender, EventArgs e)
    {
      this.InternalShutdown("Process Exit");
    }

    private void CurrentDomain_DomainUnload(object sender, EventArgs e)
    {
      this.InternalShutdown("Domain Unload");
    }

    private void Dispose(bool disposing)
    {
      if (disposing)
      {
        if (this._running)
          this.InternalShutdown("Disposing");
        this._events.Clear();
        this._logMessageSeverity.Clear();
        this._logWriteMode.Clear();
        this._methods.Clear();
        this._samplingType.Clear();
      }
      this._running = false;
      this._events = (Dictionary<Logging.LogEvents, EventInfo>) null;
      this._logMessageSeverity = (Dictionary<Logging.LogMessageSeverity, Enum>) null;
      this._logWriteMode = (Dictionary<Logging.LogWriteMode, Enum>) null;
      this._methods = (Dictionary<Logging.LogMethod, MethodInfo>) null;
      this._samplingType = (Dictionary<Logging.SamplingType, Enum>) null;
    }

    private void InternalShutdown(string message)
    {
      this._running = false;
      this._methods[Logging.LogMethod.EndSession].Invoke((object) null, new object[1]
      {
        (object) message
      });
    }

    private void InternalCounter(string name, int delta)
    {
      object obj1 = this._methods[Logging.LogMethod.RegisterSampleDefinition].Invoke((object) null, new object[7]{ (object) "Counter", (object) typeof (Logging).FullName, (object) name, (object) this._samplingType[Logging.SamplingType.IncrementalCount], null, null, null });
      if (obj1 == null)
        return;
      object obj2 = this._methods[Logging.LogMethod.RegisterSample].Invoke((object) null, new object[2]{ obj1, (object) name });
      if (obj2 == null)
        return;
      this._methods[Logging.LogMethod.WriteSample].Invoke(obj2, new object[1]
      {
        (object) (double) delta
      });
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void InternalWrite(Logging.LogMessageSeverity logMessageSeverity, int stackSteps, Exception exception, Logging.LogWriteMode logWriteMode, string detailsXml, string name, string text)
    {
      this._methods[Logging.LogMethod.Write].Invoke((object) null, new object[10]
      {
        (object) this._logMessageSeverity[logMessageSeverity],
        (object) this._name,
        (object) (stackSteps + Logging.Configuration.ReflectionStackSteps),
        (object) exception,
        (object) this._logWriteMode[logWriteMode],
        (object) detailsXml,
        (object) name,
        null,
        (object) text,
        null
      });
    }

    void IDisposable.Dispose()
    {
      GC.SuppressFinalize((object) this);
      this.Dispose(true);
    }

    internal static class Configuration
    {
      internal static readonly int ReflectionStackSteps = 5;
      internal static readonly int MaxViewerMessages = 100;
      internal static readonly string ViewerHotKey = "Ctrl-Alt-F5";
      internal static readonly string MutexName = "Local\\VistaDB_4.1_Log";
      internal static readonly string AgentAssembly = "Gibraltar.Agent, Version=3.0.0.0, Culture=neutral, PublicKeyToken=ca42a1ee8d2e42d3";
      internal static readonly string AgentType = "Gibraltar.Agent.Log";
      internal static bool IsWeb = false;
      private static string _logPath = (string) null;
      internal static bool LogCommandSQL = false;
      internal static bool LogReaderRows = false;
      internal const int StackStepsAttributeHere = 0;
      internal const int StackStepsOurOuterCaller = 1;
      private const string PublicKeyToken = "dfc935afe2125461";
      private const string AssemblyVersion = "4.1.0.0";

      internal static string GetLogPath()
      {
        return Logging.Configuration._logPath;
      }
    }

    private enum LogMessageSeverity
    {
      Verbose,
      Information,
      Error,
      Critical,
    }

    private enum LogWriteMode
    {
      Queued,
    }

    private enum SamplingType
    {
      IncrementalCount,
    }

    private enum LogMethod
    {
      Write,
      StartSession,
      EndSession,
      RegisterSampleDefinition,
      RegisterSample,
      WriteSample,
    }

    private enum LogEvents
    {
      Initializing,
    }

    internal class ReflectedProperties
    {
      private Type _type;
      private object _obj;
      private Dictionary<string, PropertyInfo> _properties;

      internal ReflectedProperties(EventArgs args)
        : this((object) args)
      {
      }

      private ReflectedProperties(object obj)
      {
        this._type = obj.GetType();
        this._obj = obj;
        this._properties = new Dictionary<string, PropertyInfo>(10);
      }

      internal Logging.ReflectedProperties this[string name]
      {
        get
        {
          PropertyInfo property = this.GetProperty(name);
          if (property != null)
            return new Logging.ReflectedProperties(property.GetValue(this._obj, (object[]) null));
          return (Logging.ReflectedProperties) null;
        }
      }

      internal T GetValue<T>(string name)
      {
        PropertyInfo property = this.GetProperty(name);
        if (property == null)
          throw new ArgumentException(name + " was not found", nameof (name));
        if (property.PropertyType != typeof (T))
          throw new ArgumentException("Argument type mismatch", "value");
        return (T) property.GetValue(this._obj, (object[]) null);
      }

      internal T GetValue<T>(string name, T defaultValue)
      {
        PropertyInfo property = this.GetProperty(name);
        if (property == null || property.PropertyType != typeof (T))
          return defaultValue;
        return (T) property.GetValue(this._obj, (object[]) null);
      }

      internal void SetValue<T>(string name, T value)
      {
        PropertyInfo property = this.GetProperty(name);
        if (property == null)
          throw new ArgumentException(name + " was not found", nameof (name));
        if (property.PropertyType != typeof (T))
          throw new ArgumentException("Argument type mismatch", nameof (value));
        property.SetValue(this._obj, (object) value, (object[]) null);
      }

      private PropertyInfo GetProperty(string name)
      {
        PropertyInfo propertyInfo;
        if (this._properties.TryGetValue(name, out propertyInfo))
          return propertyInfo;
        PropertyInfo property = this._type.GetProperty(name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        if (property != null)
          this._properties.Add(name, property);
        return property;
      }
    }
  }
}
