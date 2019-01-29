using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal interface ITableValuedFunction
  {
    VistaDBType[] GetResultColumnTypes();

    string[] GetResultColumnNames();

    void Open();

    bool First(IRow row);

    bool GetNextResult(IRow row);

    void Close();

    SignatureType Prepare();
  }
}
