using System;
using System.Data;

namespace VistaDB.Engine.Internal
{
  internal interface IQuerySchemaInfo
  {
    string GetAliasName(int ordinal);

    int GetColumnOrdinal(string name);

    int GetWidth(int ordinal);

    bool GetIsKey(int ordinal);

    string GetColumnName(int ordinal);

    string GetTableName(int ordinal);

    Type GetColumnType(int ordinal);

    bool GetIsAllowNull(int ordinal);

    VistaDBType GetColumnVistaDBType(int ordinal);

    bool GetIsAliased(int ordinal);

    bool GetIsExpression(int ordinal);

    bool GetIsAutoIncrement(int ordinal);

    bool GetIsLong(int ordinal);

    bool GetIsReadOnly(int ordinal);

    string GetDataTypeName(int ordinal);

    string GetColumnDescription(int ordinal);

    string GetColumnCaption(int ordinal);

    bool GetIsEncrypted(int ordinal);

    int GetCodePage(int ordinal);

    string GetIdentity(int ordinal, out string step, out string seed);

    string GetDefaultValue(int ordinal, out bool useInUpdate);

    DataTable GetSchemaTable();

    int ColumnCount { get; }
  }
}
