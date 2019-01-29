using System;
using System.Collections;
using System.Collections.Generic;

namespace VistaDB.DDA
{
  public interface IVistaDBTableSchema : IVistaDBDatabaseObject, IEnumerable<IVistaDBColumnAttributes>, IEnumerable, IDisposable
  {
    new string Name { get; set; }

    new string Description { get; set; }

    int ColumnCount { get; }

    IVistaDBColumnAttributes AddColumn(string name, VistaDBType type);

    IVistaDBColumnAttributes AddColumn(string name, VistaDBType type, int maxLen, int codePage);

    IVistaDBColumnAttributes AddColumn(string name, VistaDBType type, int maxLen);

    void DropColumn(string name);

    IVistaDBColumnAttributes DefineColumnAttributes(string name, bool allowNull, bool readOnly, bool encrypted, bool packed, string caption, string description);

    IVistaDBColumnAttributes AlterColumnName(string oldName, string newName);

    IVistaDBColumnAttributes AlterColumnType(string name, VistaDBType newType, int newMaxLen, int newCodePage);

    IVistaDBColumnAttributes AlterColumnType(string name, VistaDBType newType);

    IVistaDBColumnAttributes AlterColumnOrder(string name, int order);

    ICollection<string> DroppedColumns { get; }

    ICollection<string> RenamedColumns { get; }

    IVistaDBIndexCollection Indexes { get; }

    IVistaDBIdentityCollection Identities { get; }

    IVistaDBDefaultValueCollection DefaultValues { get; }

    IVistaDBConstraintCollection Constraints { get; }

    IVistaDBClrTriggerCollection Triggers { get; }

    IVistaDBRelationshipCollection ForeignKeys { get; }

    IVistaDBColumnAttributes this[string columnName] { get; }

    IVistaDBColumnAttributes this[int rowIndex] { get; }

    IVistaDBIndexInformation DefineIndex(string name, string keyExpression, bool primary, bool unique);

    void DropIndex(string name);

    void DefineIdentity(string columnName, string seedValue, string stepExpression);

    void DropIdentity(string columnName);

    void DefineDefaultValue(string columnName, string scriptExpression, bool useInUpdate, string description);

    void DropDefaultValue(string columnName);

    void DefineConstraint(string name, string scriptExpression, string description, bool insert, bool update, bool delete);

    void DropConstraint(string name);

    bool IsSynchronized { get; }

    bool IsTombstoneTable { get; }

    bool IsSystemTable { get; }
  }
}
