using System;
using System.Data;
using System.Globalization;
using System.Xml;

namespace VistaDB.DDA
{
  public interface IVistaDBDatabase : IVistaDBTable, IDisposable
  {
    CultureInfo Culture { get; }

    bool CaseSensitive { get; }

    int PageSize { get; }

    VistaDBDatabaseOpenMode Mode { get; }

    bool IsolatedStorage { get; }

    string Description { get; set; }

    int NestedTransactionLevel { get; }

    IsolationLevel IsolationLevel { get; }

    IVistaDBTableNameCollection GetTableNames();

    bool ContainsTable(string Tablename);

    IVistaDBTable OpenTable(string name, bool exclusive, bool readOnly);

    IVistaDBTableSchema TableSchema(string name);

    IVistaDBTableSchema NewTable(string name);

    IVistaDBTable CreateTable(IVistaDBTableSchema schema, bool exclusive, bool readOnly);

    void AlterTable(string oldName, IVistaDBTableSchema schema);

    void DropTable(string name);

    bool TestDatabaseObjectName(string name, bool raiseException);

    IVistaDBRelationshipCollection Relationships { get; }

    void ExportXml(string xmlFileName, VistaDBXmlWriteMode mode);

    void ImportXml(string xmlFileName, VistaDBXmlReadMode mode, bool interruptOnError);

    void ImportXml(XmlReader xmlReader, VistaDBXmlReadMode mode, bool interruptOnError);

    void AddToXmlTransferList(string tableName);

    void ClearXmlTransferList();

    IVistaDBColumn GetLastIdentity(string tableName, string columnName);

    IVistaDBColumn GetLastTimestamp(string tableName);

    void BeginTransaction();

    void BeginTransaction(IsolationLevel level);

    void CommitTransaction();

    void RollbackTransaction();

    void AddAssembly(string assemblyName, string assemblyFileName, string description);

    void UpdateAssembly(string assemblyName, string assemblyFileName, string description);

    void DropAssembly(string assemblyName, bool force);

    IVistaDBAssemblyCollection GetAssemblies();

    IVistaDBClrProcedureCollection GetClrProcedures();

    IVistaDBClrTriggerCollection GetClrTriggers();

    IVistaDBClrTriggerCollection GetClrTriggers(string tableName);

    void RegisterClrProcedure(string procedureName, string clrHostedMethod, string assemblyName, string description);

    void UnregisterClrProcedure(string procedureName);

    void PrepareClrContext();

    void PrepareClrContext(IVistaDBPipe pipe);

    void UnprepareClrContext();

    object InvokeClrProcedure(string procedureName, params object[] parameters);

    object InvokeClrProcedureFillRow(string procedureName, params object[] parameters);

    void RegisterClrTrigger(string triggerName, string clrHostedMethod, string assemblyName, string tableName, TriggerAction triggerAction, string description);

    void UnregisterClrTrigger(string triggerName);

    void ActivateSyncService(string tableName);

    void DeactivateSyncService(string tableName);

    Guid VersionGuid { get; }
  }
}
