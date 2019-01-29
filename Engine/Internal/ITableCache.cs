using System.Collections.Generic;

namespace VistaDB.Engine.Internal
{
  internal interface ITableCache
  {
    object[] GetValues(object key);

    void SetValues(object key, object[] values);

    ColumnCache GetColumnCache(string returnColumnName);

    void RegisterColumnSignature(int columnIndex);

    bool IsColumnSignatureRegistered(int columnIndex);

    IEnumerable<int> GetRegisteredColumns();

    void Close();
  }
}
