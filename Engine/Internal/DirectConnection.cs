﻿





using System;
using System.Collections.Generic;
using System.IO;
using System.Security;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core;
using VistaDB.Engine.Core.Cryptography;
using VistaDB.Engine.Core.IO;

namespace VistaDB.Engine.Internal
{
  internal class DirectConnection : Connection, IVistaDBDDA, IVistaDBConnection, IDisposable
  {
    private static readonly StorageManager storageManager = new StorageManager();
    private Dictionary<ulong, DatabaseMetaTable> databases = new Dictionary<ulong, DatabaseMetaTable>();

        internal static DirectConnection CreateInstance(VistaDBEngine engine, long id)
    {
      return new DirectConnection(engine, id);
    }

    protected DirectConnection(VistaDBEngine engine, long id)
      : base(engine, id)
    {
    }

    private void CloseDatabases()
    {
      if (databases.Count == 0)
        return;
      foreach (DatabaseMetaTable databaseMetaTable in new List<DatabaseMetaTable>(databases.Values))
        databaseMetaTable?.Dispose();
    }

    private static string TreatDataPath(string fileName)
    {
      if (fileName.StartsWith("|DataDirectory|", StringComparison.OrdinalIgnoreCase))
      {
        object data = AppDomain.CurrentDomain.GetData("DataDirectory");
        string path1 = data as string;
        if (data != null && path1 == null)
          throw new VistaDBException(0);
        if (string.IsNullOrEmpty(path1))
          path1 = AppDomain.CurrentDomain.BaseDirectory;
        if (path1 == null)
          path1 = "";
        int length = "|DataDirectory|".Length;
        while (fileName[length] == Path.DirectorySeparatorChar && length < fileName.Length)
          ++length;
        fileName = Path.Combine(path1, fileName.Substring(length));
      }
      return fileName;
    }

    internal StorageManager StorageManager
    {
      get
      {
        return storageManager;
      }
    }

    internal DataStorage LookForTable(char[] buffer, int offset, bool containSpaces)
    {
      return null;
    }

    internal DataStorage LookForTable(string name)
    {
      return null;
    }

    internal static bool IsCorrectAlias(string name)
    {
      if (name.Length == 0)
        return false;
      int index1 = 0;
      int length = name.Length;
      if (name[index1] == '[' && name[length - 1] == ']')
      {
        ++index1;
        --length;
      }
      if (length <= index1)
        return false;
      bool firstPosition = true;
      for (int index2 = index1; index2 < length; ++index2)
      {
        if (!IsCorrectNameSymbol(name[index2], ref firstPosition, true))
          return false;
      }
      return true;
    }

    internal static bool IsCorrectNameSymbol(char ch, ref bool firstPosition, bool mayContainSpace)
    {
      try
      {
        return char.IsLetter(ch) || ch == '_' || ch == '#' || !firstPosition && (char.IsNumber(ch) || ch == '@') || mayContainSpace && char.IsWhiteSpace(ch);
      }
      finally
      {
        firstPosition = false;
      }
    }

    internal void UnregisterDatabase(DatabaseMetaTable database)
    {
      if (database == null)
        return;
      databases.Remove(database.Id);
    }

    internal IVistaDBDatabase CreateDatabase(string fileName, bool stayExclusive, string cryptoKeyString, int pageSize, int LCID, bool caseSensitive, bool inMemory, bool isolated)
    {
      if (!inMemory)
        fileName = TreatDataPath(fileName);
      try
      {
        EncryptionKey cryptoKey = EncryptionKey.Create(cryptoKeyString);
        DatabaseMetaTable instance = DatabaseMetaTable.CreateInstance(fileName, this, cryptoKey, pageSize, LCID, caseSensitive, false);
        instance.Create(stayExclusive, inMemory, isolated);
        databases.Add(instance.Id, instance);
        return instance;
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 113, fileName);
      }
    }

    internal string TemporaryPath { get; set; }

    public IVistaDBDatabase CreateInMemoryDatabase(string cryptoKeyString, int pageSize, int LCID, bool caseSensitive)
    {
      try
      {
        string str;
        try
        {
          str = Path.GetTempFileName();
          File.Delete(str);
        }
        catch (SecurityException)
                {
          str = Path.Combine(TemporaryPath, "VistaDB." + Guid.NewGuid().ToString() + ".tmp");
        }
        return CreateDatabase(str, true, cryptoKeyString, pageSize, LCID, caseSensitive, true, false);
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 115);
      }
    }

    public IVistaDBDatabase CreateDatabase(string fileName, bool stayExclusive, string cryptoKeyString, int pageSize, int LCID, bool caseSensitive)
    {
      return CreateDatabase(fileName, stayExclusive, cryptoKeyString, pageSize, LCID, caseSensitive, false, false);
    }

    public IVistaDBDatabase CreateInMemoryDatabase(string cryptoKeyString, int LCID, bool caseSensitive)
    {
      return CreateInMemoryDatabase(cryptoKeyString, 0, LCID, caseSensitive);
    }

    public IVistaDBDatabase CreateIsolatedDatabase(string fileName, string cryptoKeyString, int pageSize, int LCID, bool caseSensitive)
    {
      try
      {
        return CreateDatabase(fileName, true, cryptoKeyString, pageSize, LCID, caseSensitive, false, true);
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 114);
      }
    }

    private IVistaDBDatabase OpenDatabase(string fileName, VistaDBDatabaseOpenMode mode, string cryptoKeyString, bool toPack, bool isolated)
    {
      fileName = TreatDataPath(fileName);
      try
      {
        EncryptionKey cryptoKey = EncryptionKey.Create(cryptoKeyString);
        DatabaseMetaTable instance = DatabaseMetaTable.CreateInstance(fileName, this, cryptoKey, 1, sbyte.MaxValue, false, toPack);
        bool exclusive = mode == VistaDBDatabaseOpenMode.ExclusiveReadWrite || mode == VistaDBDatabaseOpenMode.ExclusiveReadOnly;
        bool readOnly = mode == VistaDBDatabaseOpenMode.ExclusiveReadOnly || mode == VistaDBDatabaseOpenMode.NonexclusiveReadOnly || mode == VistaDBDatabaseOpenMode.SharedReadOnly;
        bool readOnlyShared = mode == VistaDBDatabaseOpenMode.SharedReadOnly;
        instance.Open(exclusive, readOnly, readOnlyShared, isolated);
        if (!toPack)
        {
          int num = instance.Rowset.PageSize / StorageHandle.DEFAULT_SIZE_OF_PAGE;
        }
        databases.Add(instance.Id, instance);
        return instance;
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 117, fileName);
      }
    }

    public IVistaDBDatabase OpenDatabase(string fileName, VistaDBDatabaseOpenMode mode, string cryptoKeyString)
    {
      return OpenDatabase(fileName, mode, cryptoKeyString, false, false);
    }

    public IVistaDBDatabase OpenIsolatedDatabase(string fileName, VistaDBDatabaseOpenMode mode, string cryptoKeyString)
    {
      try
      {
        if (mode != VistaDBDatabaseOpenMode.ExclusiveReadOnly && mode != VistaDBDatabaseOpenMode.ExclusiveReadWrite && mode != VistaDBDatabaseOpenMode.SharedReadOnly)
          throw new VistaDBException(158, mode.ToString());
        return OpenDatabase(fileName, mode, cryptoKeyString, false, true);
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 118);
      }
    }

    private DatabaseMetaTable PrepareShrinking(string fileName, string cryptoKeyString, bool backup)
    {
      if (backup)
        File.Copy(fileName, fileName + ".backupCopy", true);
      return (DatabaseMetaTable) OpenDatabase(fileName, VistaDBDatabaseOpenMode.ExclusiveReadWrite, cryptoKeyString, true, false);
    }

    private void Repair(string fileName, string cryptoKeyString, OperationCallbackDelegate operationCallback)
    {
      PackDatabaseInternal(fileName, cryptoKeyString, false, operationCallback, true);
    }

    private void PackDatabaseInternal(string fileName, string cryptoKeyString, bool backup, OperationCallbackDelegate operationCallbackDelegate, bool repair)
    {
      try
      {
        using (DatabaseMetaTable databaseMetaTable = PrepareShrinking(fileName, cryptoKeyString, backup))
        {
          databaseMetaTable.SetRepairMode(repair);
          //databaseMetaTable.SetOperationCallbackDelegate(operationCallbackDelegate);
          databaseMetaTable.Pack();
        }
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 336, fileName);
      }
    }

    private void PackDatabaseInternal(string fileName, string cryptoKeyString, string newcryptoKeyString, int newPageSize, int newLCID, bool newCaseSensitive, bool backup, OperationCallbackDelegate operationCallbackDelegate)
    {
      try
      {
        using (DatabaseMetaTable databaseMetaTable = PrepareShrinking(fileName, cryptoKeyString, backup))
        {
          //databaseMetaTable.SetOperationCallbackDelegate(operationCallbackDelegate);
          databaseMetaTable.Pack(newcryptoKeyString, newPageSize, newLCID, newCaseSensitive);
        }
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 336, fileName);
      }
    }

    void IVistaDBDDA.PackDatabase(string fileName, string cryptoKeyString, bool backup, OperationCallbackDelegate operationCallback)
    {
      PackDatabaseInternal(fileName, cryptoKeyString, backup, operationCallback, false);
    }

    void IVistaDBDDA.PackDatabase(string fileName)
    {
      PackDatabaseInternal(fileName, null, false, null, false);
    }

    void IVistaDBDDA.PackDatabase(string fileName, OperationCallbackDelegate operationCallback)
    {
      PackDatabaseInternal(fileName, null, false, operationCallback, false);
    }

    void IVistaDBDDA.PackDatabase(string fileName, string currentcryptoKeyString, string newcryptoKeyString, int newPageSize, int newLCID, bool newCaseSensitive, bool backup, OperationCallbackDelegate operationCallback)
    {
      PackDatabaseInternal(fileName, currentcryptoKeyString, newcryptoKeyString, newPageSize, newLCID, newCaseSensitive, backup, operationCallback);
    }

    void IVistaDBDDA.RepairDatabase(string fileName, string cryptoKeyString, OperationCallbackDelegate operationCallback)
    {
      Repair(fileName, cryptoKeyString, operationCallback);
    }

    void IVistaDBDDA.RepairDatabase(string fileName, OperationCallbackDelegate operationCallback)
    {
      Repair(fileName, null, operationCallback);
    }

    void IVistaDBDDA.RepairDatabase(string fileName)
    {
      Repair(fileName, null, null);
    }

    protected override void Dispose(bool disposing)
    {
      lock (SyncRoot)
      {
        if (disposing)
        {
          try
          {
            CloseDatabases();
          }
          finally
          {
            databases.Clear();
          }
        }
        base.Dispose(disposing);
      }
    }
  }
}
