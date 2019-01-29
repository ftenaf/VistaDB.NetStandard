using System;

namespace VistaDB.DDA
{
  [Flags]
  public enum VistaDBOperationStatusTypes
  {
    IndexOperation = 0,
    DataExportOperation = 1,
    DataImportOperation = 2,
    SchemaChangeOperation = DataImportOperation | DataExportOperation, // 0x00000003
    SyncDataOperation = 4,
    SyncSchemaOperation = SyncDataOperation | DataExportOperation, // 0x00000005
    FullTextOperation = SyncDataOperation | DataImportOperation, // 0x00000006
    ConstraintOperation = FullTextOperation | DataExportOperation, // 0x00000007
    BinaryOperation = 8,
    EncryptOperation = BinaryOperation | DataExportOperation, // 0x00000009
    TransactionOperation = BinaryOperation | DataImportOperation, // 0x0000000A
    SqlProcOperation = TransactionOperation | DataExportOperation, // 0x0000000B
    ClrProcOperation = BinaryOperation | SyncDataOperation, // 0x0000000C
    BackupOperation = ClrProcOperation | DataExportOperation, // 0x0000000D
    RowOperation = ClrProcOperation | DataImportOperation, // 0x0000000E
    ViewOperation = RowOperation | DataExportOperation, // 0x0000000F
    FreeSpaceOperation = 16, // 0x00000010
  }
}
