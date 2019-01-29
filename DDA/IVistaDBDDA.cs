using System;

namespace VistaDB.DDA
{
  public interface IVistaDBDDA : IVistaDBConnection, IDisposable
  {
    IVistaDBDatabase CreateDatabase(string fileName, bool stayExclusive, string encryptionKeyString, int pageSize, int LCID, bool caseSensitive);

    IVistaDBDatabase CreateIsolatedDatabase(string fileName, string encryptionKeyString, int pageSize, int LCID, bool caseSensitive);

    IVistaDBDatabase CreateInMemoryDatabase(string encryptionKeyString, int LCID, bool caseSensitive);

    IVistaDBDatabase CreateInMemoryDatabase(string encryptionKeyString, int pageSize, int LCID, bool caseSensitive);

    IVistaDBDatabase OpenDatabase(string fileName, VistaDBDatabaseOpenMode mode, string encryptionKeyString);

    IVistaDBDatabase OpenIsolatedDatabase(string fileName, VistaDBDatabaseOpenMode mode, string encryptionKeyString);

    void PackDatabase(string fileName, string encryptionKeyString, bool backup, OperationCallbackDelegate operationCallbackDelegate);

    void PackDatabase(string fileName, string encryptionKeyString, string newencryptionKeyString, int newPageSize, int newLCID, bool newCaseSensitive, bool backup, OperationCallbackDelegate operationCallbackDelegate);

    void PackDatabase(string fileName, OperationCallbackDelegate operationCallback);

    void PackDatabase(string fileName);

    void RepairDatabase(string fileName, string encryptionKeyString, OperationCallbackDelegate operationCallbackDelegate);

    void RepairDatabase(string fileName, OperationCallbackDelegate operationCallback);

    void RepairDatabase(string fileName);
  }
}
