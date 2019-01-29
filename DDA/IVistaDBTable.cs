using System;
using System.Diagnostics;

namespace VistaDB.DDA
{
  public interface IVistaDBTable : IDisposable
  {
    string Name { get; }

    string ActiveIndex { get; set; }

    void Close();

    bool IsClosed { get; }

    IVistaDBRow Evaluate(string expression);

    void SetFilter(string expression, bool optimize);

    void ResetFilter();

    string GetFilter(bool optimizable);

    void GetScope(out IVistaDBRow lowKey, out IVistaDBRow highKey);

    void SetScope(IVistaDBRow lowKey, IVistaDBRow highKey);

    void SetScope(string lowKeyExpression, string highKeyExpression);

    void ResetScope();

    [DebuggerBrowsable(DebuggerBrowsableState.Never)]
    long ScopeKeyCount { get; }

    bool Find(string keyEvaluationExpression, string indexName, bool partialMatching, bool softPosition);

    bool Find(IVistaDBRow key, string indexName, bool partialMatching, bool softPosition);

    void CreateIndex(string name, string keyExpression, bool primary, bool unique);

    void CreateTemporaryIndex(string name, string keyExpression, bool unique);

    void CreateFTSIndex(string name, string columns);

    void DropIndex(string name);

    void DropFTSIndex();

    void RenameIndex(string oldName, string newName);

    void CreateIdentity(string columnName, string seedExpression, string stepExpression);

    void DropIdentity(string columnName);

    void CreateDefaultValue(string columnName, string scriptExpression, bool useInUpdate, string description);

    void DropDefaultValue(string columnName);

    void CreateConstraint(string name, string scriptExpression, string description, bool insertion, bool update, bool delete);

    void DropConstraint(string name);

    void CreateForeignKey(string constraintName, string foreignKey, string primaryTable, VistaDBReferentialIntegrity updateIntegrity, VistaDBReferentialIntegrity deleteIntegrity, string description);

    void DropForeignKey(string constraintName);

    void Put(int index, object columnValue);

    void Put(string columnName, IVistaDBValue columnValue);

    void Put(int index, IVistaDBValue columnValue);

    void PutString(string columnName, string value);

    void PutString(int index, string value);

    void PutByte(string columnName, byte value);

    void PutByte(int index, byte value);

    void PutInt16(string columnName, short value);

    void PutInt16(int index, short value);

    void PutInt32(string columnName, int value);

    void PutInt32(int index, int value);

    void PutInt64(string columnName, long value);

    void PutInt64(int index, long value);

    void PutSingle(string columnName, float value);

    void PutSingle(int index, float value);

    void PutDouble(string columnName, double value);

    void PutDouble(int index, double value);

    void PutDecimal(string columnName, Decimal value);

    void PutDecimal(int index, Decimal value);

    void PutBoolean(string columnName, bool value);

    void PutBoolean(int index, bool value);

    void PutDateTime(string columnName, DateTime value);

    void PutDateTime(int index, DateTime value);

    void PutBinary(string columnName, byte[] value);

    void PutBinary(int index, byte[] value);

    void PutGuid(string columnName, Guid value);

    void PutGuid(int index, Guid value);

    void PutNull(string columnName);

    void PutNull(int index);

    void PutFromFile(string columnName, string fileName);

    void PutFromFile(int index, string fileName);

    void GetToFile(string columnName, string fileName);

    void GetToFile(int index, string fileName);

    IVistaDBValue Get(string columnName);

    IVistaDBValue Get(int columnIndex);

    void Insert();

    void Post(bool leaveRowLock);

    void Post();

    void Delete();

    void Lock(long rowId);

    void Unlock(long rowId);

    IVistaDBRow LastSessionIdentity { get; }

    IVistaDBRow LastTableIdentity { get; }

    void First();

    void Last();

    void Prev();

    void Next();

    void MoveBy(int rowNumber);

    IVistaDBRow CurrentKey { get; set; }

    IVistaDBRow CurrentRow { get; set; }

    long RowCount { get; }

    bool EndOfTable { get; }

    bool StartOfTable { get; }

    bool EnforceConstraints { get; set; }

    bool EnforceIdentities { get; set; }

    void ExportData(IVistaDBTable table, string constraint);

    void SetOperationCallbackDelegate(OperationCallbackDelegate operationCallbackDelegate);

    void SetDDAEventDelegate(IVistaDBDDAEventDelegate eventDelegate);

    void ResetEventDelegate(DDAEventDelegateType eventType);

    IVistaDBIndexCollection TemporaryIndexes { get; }

    IVistaDBIndexCollection RegularIndexes { get; }
  }
}
