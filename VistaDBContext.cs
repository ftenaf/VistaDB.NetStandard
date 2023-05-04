using System;
using System.Collections.Generic;
using VistaDB.DDA;
using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;
using VistaDB.Provider;

namespace VistaDB
{
  public class VistaDBContext
  {
    public class SQLChannel
    {
      private static Context sqlContext = new Context();

      public static bool IsAvailable
      {
        get
        {
          return sqlContext.Available;
        }
      }

      public static VistaDBPipe Pipe
      {
        get
        {
          return ((SQLContextData)sqlContext.CurrentContext).Pipe;
        }
      }

      public static TriggerContext TriggerContext
      {
        get
        {
          return ((SQLContextData)sqlContext.CurrentContext)?.TriggerContext;
        }
      }

      public static VistaDBTransaction CurrentTransaction
      {
        get
        {
                    SQLContextData currentContext = (SQLContextData)sqlContext.CurrentContext;
          if (currentContext == null || currentContext.LocalConnection == null)
            return (VistaDBTransaction) null;
          return currentContext.LocalConnection.CurrentTransaction;
        }
      }

      internal static ILocalSQLConnection CurrentConnection
      {
        get
        {
          return ((SQLContextData)sqlContext.CurrentContext)?.LocalConnection;
        }
      }

      internal static void ActivateContext(ILocalSQLConnection connection, VistaDBPipe pipe)
      {
                sqlContext.PushContext((IDisposable) new SQLContextData(connection, pipe, TriggerContext));
      }

      internal static void DeactivateContext()
      {
                sqlContext.PopContext();
      }

      internal static void PushTriggerContext(Table[] modificationTables, TriggerAction action, int columnCount)
      {
        ((SQLContextData)sqlContext.CurrentContext).PushTriggerContext(modificationTables, action, columnCount);
      }

      internal static void PopTriggerContext()
      {
        ((SQLContextData)sqlContext.CurrentContext).PopTriggerContext();
      }

      internal void RegisterActiveTrigger(string tableName, TriggerAction type)
      {
        if (!IsAvailable)
          return;
                CurrentConnection.RegisterTrigger(tableName, type);
      }

      internal void UnregisterActiveTrigger(string tableName, TriggerAction type)
      {
        if (!IsAvailable)
          return;
                CurrentConnection.UnregisterTrigger(tableName, type);
      }

      internal bool IsActiveTrigger(string tableName, TriggerAction type)
      {
        if (IsAvailable)
          return CurrentConnection.IsTriggerActing(tableName, type);
        return false;
      }

      private class SQLContextData : IDisposable
      {
        private Stack<TriggerContext> triggerStack = new Stack<TriggerContext>();
        private ILocalSQLConnection connection;
        private VistaDBPipe pipe;

        internal SQLContextData(ILocalSQLConnection connection, VistaDBPipe pipe, TriggerContext currentTrigger)
        {
          this.connection = connection;
          this.pipe = pipe;
          triggerStack.Push(currentTrigger);
        }

        internal ILocalSQLConnection LocalConnection
        {
          get
          {
            return connection;
          }
        }

        internal VistaDBPipe Pipe
        {
          get
          {
            return pipe;
          }
        }

        internal TriggerContext TriggerContext
        {
          get
          {
            return triggerStack.Peek();
          }
        }

        internal void PushTriggerContext(Table[] modificationTables, TriggerAction action, int columnCount)
        {
          triggerStack.Push(new TriggerContext(modificationTables, action, columnCount));
        }

        internal void PopTriggerContext()
        {
          triggerStack.Pop();
        }

        public void Dispose()
        {
          triggerStack.Clear();
          GC.SuppressFinalize((object) this);
        }
      }
    }

    public class DDAChannel
    {
      private static Context ddaContext = new Context();

      public static bool IsAvailable
      {
        get
        {
          return ddaContext.Available;
        }
      }

      public static IVistaDBPipe Pipe
      {
        get
        {
          return ((DDAContextData)ddaContext.CurrentContext).Pipe;
        }
      }

      public static IVistaDBDatabase CurrentDatabase
      {
        get
        {
          return ((DDAContextData)ddaContext.CurrentContext).Database;
        }
      }

      internal static void ActivateContext(IVistaDBDatabase database, IVistaDBPipe pipe)
      {
                ddaContext.PushContext((IDisposable) new DDAContextData(database, pipe));
      }

      internal static void DeactivateContext()
      {
                ddaContext.PopContext();
      }

      private class DDAContextData : IDisposable
      {
        private IVistaDBDatabase database;
        private IVistaDBPipe pipe;

        internal DDAContextData(IVistaDBDatabase database, IVistaDBPipe pipe)
        {
          this.database = database;
          this.pipe = pipe;
        }

        internal IVistaDBDatabase Database
        {
          get
          {
            return database;
          }
        }

        internal IVistaDBPipe Pipe
        {
          get
          {
            return pipe;
          }
        }

        public void Dispose()
        {
          database = (IVistaDBDatabase) null;
          pipe = (IVistaDBPipe) null;
          GC.SuppressFinalize((object) this);
        }
      }
    }
  }
}
