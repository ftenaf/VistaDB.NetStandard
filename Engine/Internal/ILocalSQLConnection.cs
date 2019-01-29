using System;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.SQL;
using VistaDB.Provider;

namespace VistaDB.Engine.Internal
{
  internal interface ILocalSQLConnection : IVistaDBConnection, IDisposable
  {
    IQueryStatement CreateQuery(string commandText);

    void FreeQuery(IQueryStatement query);

    IQueryStatement this[long queryId] { get; }

    void OpenDatabase(string fileName, VistaDBDatabaseOpenMode mode, string cryptoKeyString, bool fromIsolatedStorage);

    void CloseDatabase();

    void CloseAllPooledTables();

    void BeginTransaction(VistaDBTransaction parentTransaction);

    void CommitTransaction();

    void RollbackTransaction();

    bool IsSyntaxCorrect(string text, out int lineNo, out int symbolNo, out string errorMessage);

    bool IsViewSyntaxCorrect(string text, out int lineNo, out int symbolNo, out string errorMessage);

    bool IsConstraintSyntaxCorrect(string text, out int lineNo, out int symbolNo, out string errorMessage);

    bool TryToCorrect(string oldText, out string newText, out int lineNo, out int symbolNo, out string errorMessage);

    bool DatabaseOpened { get; }

    string FileName { get; }

    VistaDBDatabaseOpenMode OpenMode { get; }

    string Password { get; }

    VistaDBException LastException { get; set; }

    IVistaDBTableSchema TableSchema(string tableName);

    IVistaDBTableNameCollection GetTables();

    IVistaDBRelationshipCollection Relationships { get; }

    bool IsDatabaseOwner { get; }

    bool IsolatedStorage { get; }

    void RegisterTrigger(string tableName, TriggerAction type);

    void UnregisterTrigger(string tableName, TriggerAction type);

    bool IsTriggerActing(string tableName, TriggerAction type);

    VistaDBTransaction CurrentTransaction { get; }

    VistaDBConnection ParentConnection { get; }

    IQueryStatement CreateMessageQuery(string message);

    IQueryStatement CreateResultQuery(TemporaryResultSet resultSet);
  }
}
