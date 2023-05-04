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
        private static Logging Instance = null;
        private static string _productName;
        private readonly string _name;
        private bool _running;
        private Dictionary<LogMessageSeverity, Enum> _logMessageSeverity;
        private Dictionary<LogWriteMode, Enum> _logWriteMode;
        private Dictionary<SamplingType, Enum> _samplingType;
        private Dictionary<LogMethod, MethodInfo> _methods;
        private Dictionary<LogEvents, EventInfo> _events;

        internal static IDisposable QuickLog(string startupMessage, string shutdownMessage)
        {
            return null;
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
            if (Instance == null || !Instance._running)
                return;
            Instance.InternalShutdown(message);
        }

        internal static string GetProductName()
        {
            if (_productName == null)
            {
                object[] customAttributes = typeof(Logging).Assembly.GetCustomAttributes(typeof(AssemblyProductAttribute), false);
                _productName = customAttributes == null || customAttributes.Length <= 0 ? "Unknown" : ((AssemblyProductAttribute)customAttributes[0]).Product;
            }
            return _productName;
        }

        [Conditional("LOG")]
        internal static void SetupConfiguration(EventArgs args)
        {
        }

        [Conditional("LOG")]
        internal static void SetupConfiguration(ReflectedProperties args)
        {
            ReflectedProperties reflectedProperties1 = args["Configuration"];
            ReflectedProperties reflectedProperties2 = reflectedProperties1["Publisher"];
            reflectedProperties2.SetValue("ProductName", GetProductName());
            Assembly assembly = Assembly.GetEntryAssembly() ?? Assembly.GetExecutingAssembly();
            if (assembly != null)
            {
                object[] customAttributes1 = assembly.GetCustomAttributes(typeof(AssemblyTitleAttribute), false);
                if (customAttributes1 != null && customAttributes1.Length > 0)
                    reflectedProperties2.SetValue("ApplicationName", ((AssemblyTitleAttribute)customAttributes1[0]).Title);
                object[] customAttributes2 = assembly.GetCustomAttributes(typeof(AssemblyDescriptionAttribute), false);
                if (customAttributes2 != null && customAttributes2.Length > 0)
                    reflectedProperties2.SetValue("ApplicationDescription", ((AssemblyDescriptionAttribute)customAttributes2[0]).Description);
                object[] customAttributes3 = assembly.GetCustomAttributes(typeof(AssemblyVersionAttribute), false);
                if (customAttributes3 != null && customAttributes3.Length > 0)
                    reflectedProperties2.SetValue("ApplicationVersion", new Version(((AssemblyVersionAttribute)customAttributes3[0]).Version));
                object[] customAttributes4 = assembly.GetCustomAttributes(typeof(AssemblyFileVersionAttribute), false);
                if (customAttributes4 != null && customAttributes4.Length > 0)
                    reflectedProperties2.SetValue("ApplicationVersion", new Version(((AssemblyFileVersionAttribute)customAttributes4[0]).Version));
            }
            ReflectedProperties reflectedProperties3 = reflectedProperties1["Listener"];
            reflectedProperties3.SetValue("EnableNetworkEvents", false);
            reflectedProperties3.SetValue("EnableNetworkPerformance", false);
            reflectedProperties3.SetValue("EnableAssemblyEvents", false);
            reflectedProperties3.SetValue("EnableConsole", false);
            reflectedProperties3.SetValue("CatchApplicationExceptions", false);
            reflectedProperties3.SetValue("CatchUnhandledExceptions", false);
            reflectedProperties3.SetValue("ReportErrorsToUser", false);
            reflectedProperties3.SetValue("AutoTraceRegistration", false);
            reflectedProperties3.SetValue("EnableDiskPerformance", false);
            reflectedProperties3.SetValue("EnableProcessPerformance", false);
            reflectedProperties3.SetValue("EnablePowerEvents", false);
            reflectedProperties3.SetValue("EnableSystemPerformance", false);
            reflectedProperties3.SetValue("EnableUserEvents", false);
            ReflectedProperties reflectedProperties4 = reflectedProperties1["Viewer"];
            reflectedProperties4.SetValue("Enabled", false);
            reflectedProperties4.SetValue("HotKey", string.Empty);
            reflectedProperties4.SetValue("MaxMessages", Configuration.MaxViewerMessages);
            ReflectedProperties reflectedProperties5 = reflectedProperties1["SessionFile"];
            reflectedProperties5.SetValue("Folder", Configuration.GetLogPath());
            reflectedProperties5.SetValue("EnableFilePruning", false);
        }

        [Conditional("LOG")]
        internal static void Counter(string name)
        {
        }

        [Conditional("LOG")]
        internal static void Counter(string name, int delta)
        {
            if (Instance == null || !Instance._running)
                return;
            Instance.InternalCounter(name, delta);
        }

        [Conditional("LOG")]
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal static void Log(string name, string text)
        {
            if (Instance == null || !Instance._running)
                return;
            Instance.InternalWrite(LogMessageSeverity.Information, 1, null, LogWriteMode.Queued, null, name, text);
        }

        [Conditional("LOG")]
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal static void Log(string name, string text, string details)
        {
            if (Instance == null || !Instance._running)
                return;
            Instance.InternalWrite(LogMessageSeverity.Information, 1, null, LogWriteMode.Queued, details, name, text);
        }

        [Conditional("LOG")]
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal static void Log(string name, Exception exception)
        {
            if (Instance == null || !Instance._running)
                return;
            Instance.InternalWrite(LogMessageSeverity.Error, 1, exception, LogWriteMode.Queued, null, name, null);
        }

        [Conditional("LOG")]
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal static void Log(string name, Exception exception, string text)
        {
            if (Instance == null || !Instance._running)
                return;
            Instance.InternalWrite(LogMessageSeverity.Error, 1, exception, LogWriteMode.Queued, null, name, text);
        }

        [Conditional("LOG")]
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal static void Log(string name, string text, XmlDocument xml)
        {
            if (Instance == null || !Instance._running)
                return;
            Instance.InternalWrite(LogMessageSeverity.Information, 1, null, LogWriteMode.Queued, xml.OuterXml, name, text);
        }

        [Conditional("LOG")]
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal static void Log(string name, Exception exception, string text, XmlDocument xml)
        {
            if (Instance == null || !Instance._running)
                return;
            Instance.InternalWrite(LogMessageSeverity.Information, 1, exception, LogWriteMode.Queued, xml.OuterXml, name, text);
        }

        [Conditional("LOG")]
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal static void Error(string name, string text)
        {
            if (Instance == null || !Instance._running)
                return;
            Instance.InternalWrite(LogMessageSeverity.Error, 1, null, LogWriteMode.Queued, null, name, text);
        }

        [Conditional("LOG")]
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal static void Error(string name, Exception exception)
        {
            if (Instance == null || !Instance._running)
                return;
            Instance.InternalWrite(LogMessageSeverity.Critical, 1, exception, LogWriteMode.Queued, null, name, null);
        }

        [Conditional("LOG")]
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal static void Error(string name, Exception exception, string text)
        {
            if (Instance == null || !Instance._running)
                return;
            Instance.InternalWrite(LogMessageSeverity.Critical, 1, exception, LogWriteMode.Queued, null, name, text);
        }

        [Conditional("LOG")]
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal static void Error(string name, string text, XmlDocument xml)
        {
            if (Instance == null || !Instance._running)
                return;
            Instance.InternalWrite(LogMessageSeverity.Error, 1, null, LogWriteMode.Queued, xml.OuterXml, name, text);
        }

        [Conditional("LOG")]
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal static void Assert(bool condition, string name, string message)
        {
            if (Instance == null || !Instance._running || condition)
                return;
            Instance.InternalWrite(LogMessageSeverity.Verbose, 1, null, LogWriteMode.Queued, null, name, message);
        }

        [Conditional("LOG")]
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal static void Assert(bool condition, string name, Exception exception)
        {
            if (Instance == null || !Instance._running || condition && exception == null)
                return;
            Instance.InternalWrite(LogMessageSeverity.Verbose, 1, exception, LogWriteMode.Queued, null, name, null);
        }

        [Conditional("LOG")]
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal static void Assert(bool condition, string name, Exception exception, string message)
        {
            if (Instance == null || !Instance._running || condition && exception == null)
                return;
            Instance.InternalWrite(LogMessageSeverity.Verbose, 1, exception, LogWriteMode.Queued, null, name, message);
        }

        [Conditional("LOG")]
        internal static void CreateViewer(object parentControl)
        {
        }

        [Conditional("LOG")]
        internal static void CreateViewer(string name, object parentControl)
        {
            if (Instance == null)
                return;
            int num = Instance._running ? 1 : 0;
        }

        ~Logging()
        {
            Dispose(false);
        }

        private void RemoveEvent(LogEvents _event, EventHandler handler)
        {
            Delegate handler1 = Delegate.CreateDelegate(_events[_event].EventHandlerType, handler.Target, handler.Method);
            _events[_event].RemoveEventHandler(null, handler1);
        }

        private void Log_Initializing(object sender, EventArgs args)
        {
            ReflectedProperties reflectedProperties = new ReflectedProperties(args);
            AppDomain.CurrentDomain.DomainUnload += new EventHandler(CurrentDomain_DomainUnload);
            AppDomain.CurrentDomain.ProcessExit += new EventHandler(CurrentDomain_ProcessExit);
        }

        private void CurrentDomain_ProcessExit(object sender, EventArgs e)
        {
            InternalShutdown("Process Exit");
        }

        private void CurrentDomain_DomainUnload(object sender, EventArgs e)
        {
            InternalShutdown("Domain Unload");
        }

        private void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_running)
                    InternalShutdown("Disposing");
                _events.Clear();
                _logMessageSeverity.Clear();
                _logWriteMode.Clear();
                _methods.Clear();
                _samplingType.Clear();
            }
            _running = false;
            _events = null;
            _logMessageSeverity = null;
            _logWriteMode = null;
            _methods = null;
            _samplingType = null;
        }

        private void InternalShutdown(string message)
        {
            _running = false;
            _methods[LogMethod.EndSession].Invoke(null, new object[1]
            {
         message
            });
        }

        private void InternalCounter(string name, int delta)
        {
            object obj1 = _methods[LogMethod.RegisterSampleDefinition].Invoke(null, new object[7] { "Counter", typeof(Logging).FullName, name, _samplingType[SamplingType.IncrementalCount], null, null, null });
            if (obj1 == null)
                return;
            object obj2 = _methods[LogMethod.RegisterSample].Invoke(null, new object[2] { obj1, name });
            if (obj2 == null)
                return;
            _methods[LogMethod.WriteSample].Invoke(obj2, new object[1]
            {
         (double) delta
            });
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void InternalWrite(LogMessageSeverity logMessageSeverity, int stackSteps, Exception exception, LogWriteMode logWriteMode, string detailsXml, string name, string text)
        {
            _methods[LogMethod.Write].Invoke(null, new object[10]
            {
         _logMessageSeverity[logMessageSeverity],
         _name,
         stackSteps + Configuration.ReflectionStackSteps,
         exception,
         _logWriteMode[logWriteMode],
         detailsXml,
         name,
        null,
         text,
        null
            });
        }

        void IDisposable.Dispose()
        {
            GC.SuppressFinalize(this);
            Dispose(true);
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
            private static string _logPath = null;
            internal static bool LogCommandSQL = false;
            internal static bool LogReaderRows = false;
            internal const int StackStepsAttributeHere = 0;
            internal const int StackStepsOurOuterCaller = 1;

            internal static string GetLogPath()
            {
                return _logPath;
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
              : this((object)args)
            {
            }

            private ReflectedProperties(object obj)
            {
                _type = obj.GetType();
                _obj = obj;
                _properties = new Dictionary<string, PropertyInfo>(10);
            }

            internal ReflectedProperties this[string name]
            {
                get
                {
                    PropertyInfo property = GetProperty(name);
                    if (property != null)
                        return new ReflectedProperties(property.GetValue(_obj, null));
                    return null;
                }
            }

            internal T GetValue<T>(string name)
            {
                PropertyInfo property = GetProperty(name);
                if (property == null)
                    throw new ArgumentException(name + " was not found", nameof(name));
                if (property.PropertyType != typeof(T))
                    throw new ArgumentException("Argument type mismatch", "value");
                return (T)property.GetValue(_obj, null);
            }

            internal T GetValue<T>(string name, T defaultValue)
            {
                PropertyInfo property = GetProperty(name);
                if (property == null || property.PropertyType != typeof(T))
                    return defaultValue;
                return (T)property.GetValue(_obj, null);
            }

            internal void SetValue<T>(string name, T value)
            {
                PropertyInfo property = GetProperty(name);
                if (property == null)
                    throw new ArgumentException(name + " was not found", nameof(name));
                if (property.PropertyType != typeof(T))
                    throw new ArgumentException("Argument type mismatch", nameof(value));
                property.SetValue(_obj, value, null);
            }

            private PropertyInfo GetProperty(string name)
            {
                PropertyInfo propertyInfo;
                if (_properties.TryGetValue(name, out propertyInfo))
                    return propertyInfo;
                PropertyInfo property = _type.GetProperty(name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                if (property != null)
                    _properties.Add(name, property);
                return property;
            }
        }
    }
}
