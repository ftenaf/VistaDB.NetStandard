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
          return VistaDBContext.SQLChannel.sqlContext.Available;
        }
      }

      public static VistaDBPipe Pipe
      {
        get
        {
          return ((VistaDBContext.SQLChannel.SQLContextData) VistaDBContext.SQLChannel.sqlContext.CurrentContext).Pipe;
        }
      }

      public static TriggerContext TriggerContext
      {
        get
        {
          return ((VistaDBContext.SQLChannel.SQLContextData) VistaDBContext.SQLChannel.sqlContext.CurrentContext)?.TriggerContext;
        }
      }

      public static VistaDBTransaction CurrentTransaction
      {
        get
        {
          VistaDBContext.SQLChannel.SQLContextData currentContext = (VistaDBContext.SQLChannel.SQLContextData) VistaDBContext.SQLChannel.sqlContext.CurrentContext;
          if (currentContext == null || currentContext.LocalConnection == null)
            return (VistaDBTransaction) null;
          return currentContext.LocalConnection.CurrentTransaction;
        }
      }

      internal static ILocalSQLConnection CurrentConnection
      {
        get
        {
          return ((VistaDBContext.SQLChannel.SQLContextData) VistaDBContext.SQLChannel.sqlContext.CurrentContext)?.LocalConnection;
        }
      }

      internal static void ActivateContext(ILocalSQLConnection connection, VistaDBPipe pipe)
      {
        VistaDBContext.SQLChannel.sqlContext.PushContext((IDisposable) new VistaDBContext.SQLChannel.SQLContextData(connection, pipe, VistaDBContext.SQLChannel.TriggerContext));
      }

      internal static void DeactivateContext()
      {
        VistaDBContext.SQLChannel.sqlContext.PopContext();
      }

      internal static void PushTriggerContext(Table[] modificationTables, TriggerAction action, int columnCount)
      {
        ((VistaDBContext.SQLChannel.SQLContextData) VistaDBContext.SQLChannel.sqlContext.CurrentContext).PushTriggerContext(modificationTables, action, columnCount);
      }

      internal static void PopTriggerContext()
      {
        ((VistaDBContext.SQLChannel.SQLContextData) VistaDBContext.SQLChannel.sqlContext.CurrentContext).PopTriggerContext();
      }

      internal void RegisterActiveTrigger(string tableName, TriggerAction type)
      {
        if (!VistaDBContext.SQLChannel.IsAvailable)
          return;
        VistaDBContext.SQLChannel.CurrentConnection.RegisterTrigger(tableName, type);
      }

      internal void UnregisterActiveTrigger(string tableName, TriggerAction type)
      {
        if (!VistaDBContext.SQLChannel.IsAvailable)
          return;
        VistaDBContext.SQLChannel.CurrentConnection.UnregisterTrigger(tableName, type);
      }

      internal bool IsActiveTrigger(string tableName, TriggerAction type)
      {
        if (VistaDBContext.SQLChannel.IsAvailable)
          return VistaDBContext.SQLChannel.CurrentConnection.IsTriggerActing(tableName, type);
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
          this.triggerStack.Push(currentTrigger);
        }

        internal ILocalSQLConnection LocalConnection
        {
          get
          {
            return this.connection;
          }
        }

        internal VistaDBPipe Pipe
        {
          get
          {
            return this.pipe;
          }
        }

        internal TriggerContext TriggerContext
        {
          get
          {
            return this.triggerStack.Peek();
          }
        }

        internal void PushTriggerContext(Table[] modificationTables, TriggerAction action, int columnCount)
        {
          this.triggerStack.Push(new TriggerContext(modificationTables, action, columnCount));
        }

        internal void PopTriggerContext()
        {
          this.triggerStack.Pop();
        }

        public void Dispose()
        {
          this.triggerStack.Clear();
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
          return VistaDBContext.DDAChannel.ddaContext.Available;
        }
      }

      public static IVistaDBPipe Pipe
      {
        get
        {
          return ((VistaDBContext.DDAChannel.DDAContextData) VistaDBContext.DDAChannel.ddaContext.CurrentContext).Pipe;
        }
      }

      public static IVistaDBDatabase CurrentDatabase
      {
        get
        {
          return ((VistaDBContext.DDAChannel.DDAContextData) VistaDBContext.DDAChannel.ddaContext.CurrentContext).Database;
        }
      }

      internal static void ActivateContext(IVistaDBDatabase database, IVistaDBPipe pipe)
      {
        VistaDBContext.DDAChannel.ddaContext.PushContext((IDisposable) new VistaDBContext.DDAChannel.DDAContextData(database, pipe));
      }

      internal static void DeactivateContext()
      {
        VistaDBContext.DDAChannel.ddaContext.PopContext();
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
            return this.database;
          }
        }

        internal IVistaDBPipe Pipe
        {
          get
          {
            return this.pipe;
          }
        }

        public void Dispose()
        {
          this.database = (IVistaDBDatabase) null;
          this.pipe = (IVistaDBPipe) null;
          GC.SuppressFinalize((object) this);
        }
      }
    }
  }
}
