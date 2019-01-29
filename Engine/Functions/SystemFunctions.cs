using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Reflection;
using VistaDB.DDA;
using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL;
using VistaDB.Provider;
using VistaDB.VistaDBTypes;

namespace VistaDB.Engine.Functions
{
  [Obfuscation(ApplyToMembers = true, Exclude = true)]
  internal static class SystemFunctions
  {
    private static Dictionary<int, string> _objIds = new Dictionary<int, string>();
    public const string DefaultSchema = "dbo";

    private static Database GetCurrentDatabase()
    {
      if (VistaDBContext.DDAChannel.IsAvailable)
        return (Database) ((Table) VistaDBContext.DDAChannel.CurrentDatabase).Rowset;
      if (VistaDBContext.SQLChannel.IsAvailable)
        return (Database) VistaDBContext.SQLChannel.CurrentConnection;
      return (Database) null;
    }

    private static VistaDBConnection GetCurrentConnection()
    {
      return new VistaDBConnection("Context connection= true");
    }

    private static string GetDatabaseName(Database database)
    {
      return SystemFunctions.GetDatabaseName(database.Name);
    }

    private static string GetDatabaseName(string filename)
    {
      if (!File.Exists(filename))
        return string.Empty;
      FileInfo fileInfo = new FileInfo(filename);
      if (fileInfo.Extension.Length > 0)
        return fileInfo.Name.Remove(fileInfo.Name.Length - fileInfo.Extension.Length);
      return fileInfo.Name;
    }

    private static Assembly GetAssembly()
    {
      return typeof (SystemFunctions).Assembly;
    }

    private static ClrHosting.ClrProcedure GetMethod(string name, Assembly assembly)
    {
      Type type = typeof (SystemFunctions);
      MethodInfo method = type.GetMethod(name);
      MethodInfo fillRowProcedure = (MethodInfo) null;
      if (method == null)
        return (ClrHosting.ClrProcedure) null;
      foreach (VistaDBClrProcedureAttribute customAttribute in method.GetCustomAttributes(typeof (VistaDBClrProcedureAttribute), false))
      {
        string name1 = customAttribute.FillRow;
        if (name1 != null)
        {
          if (name1.StartsWith(type.FullName.Replace('+', '.')))
            name1 = name1.Substring(type.FullName.Length + 1);
          fillRowProcedure = type.GetMethod(name1);
          if (fillRowProcedure != null)
            break;
        }
      }
      return new ClrHosting.ClrProcedure(type.FullName + "." + method.Name, assembly, method, fillRowProcedure);
    }

    private static void GetTypeInfo(string table, string column, VistaDBType type, ref int maxLength, ref int octetLength, ref int precision, ref short scale, ref short radix, ref short dateTime)
    {
      octetLength = -1;
      precision = 0;
      scale = (short) -1;
      radix = (short) 0;
      dateTime = (short) 0;
      switch (type)
      {
        case VistaDBType.Char:
          if (maxLength < 1 || maxLength > 8000)
            throw new Exception(string.Format("Broken database detected! ([{0}].[{1}] {2}) has an invalid length of {3}, must be between {4} and {5}", (object) table, (object) column, (object) type.ToString(), (object) maxLength, (object) 1, (object) 8000));
          precision = maxLength;
          octetLength = maxLength;
          break;
        case VistaDBType.NChar:
          if (maxLength < 1 || maxLength > 8000)
            throw new Exception(string.Format("Broken database detected! ([{0}].[{1}] {2}) has an invalid length of {3}, must be between {4} and {5}", (object) table, (object) column, (object) type.ToString(), (object) maxLength, (object) 1, (object) 8000));
          precision = maxLength / 2;
          octetLength = maxLength;
          break;
        case VistaDBType.VarChar:
          if (maxLength < 1 || maxLength > 8000)
            throw new Exception(string.Format("Broken database detected! ([{0}].[{1}] {2}) has an invalid length of {3}, must be between {4} and {5}", (object) table, (object) column, (object) type.ToString(), (object) maxLength, (object) 1, (object) 8000));
          precision = maxLength;
          octetLength = maxLength;
          break;
        case VistaDBType.NVarChar:
          if (maxLength < 1 || maxLength > 8000)
            throw new Exception(string.Format("Broken database detected! ([{0}].[{1}] {2}) has an invalid length of {3}, must be between {4} and {5}", (object) table, (object) column, (object) type.ToString(), (object) maxLength, (object) 1, (object) 8000));
          precision = maxLength / 2;
          octetLength = maxLength;
          break;
        case VistaDBType.Text:
          precision = int.MaxValue;
          octetLength = maxLength;
          break;
        case VistaDBType.NText:
          precision = 1073741823;
          octetLength = maxLength;
          break;
        case VistaDBType.TinyInt:
          precision = 3;
          scale = (short) 0;
          radix = (short) 10;
          break;
        case VistaDBType.SmallInt:
          precision = 5;
          scale = (short) 0;
          radix = (short) 10;
          break;
        case VistaDBType.Int:
          precision = 10;
          scale = (short) 0;
          radix = (short) 10;
          break;
        case VistaDBType.BigInt:
          precision = 19;
          scale = (short) 0;
          radix = (short) 10;
          break;
        case VistaDBType.Real:
          precision = 7;
          radix = (short) 10;
          break;
        case VistaDBType.Float:
          precision = 15;
          radix = (short) 10;
          break;
        case VistaDBType.Decimal:
          precision = 18;
          scale = (short) 0;
          radix = (short) 10;
          break;
        case VistaDBType.Money:
          precision = 19;
          scale = (short) 4;
          radix = (short) 10;
          break;
        case VistaDBType.SmallMoney:
          precision = 10;
          scale = (short) 4;
          radix = (short) 10;
          break;
        case VistaDBType.Bit:
          precision = 1;
          break;
        case VistaDBType.DateTime:
          precision = 23;
          scale = (short) 3;
          dateTime = (short) 3;
          break;
        case VistaDBType.Image:
          precision = int.MaxValue;
          octetLength = maxLength;
          break;
        case VistaDBType.VarBinary:
          if (maxLength < 1 || maxLength > 8000)
            throw new Exception(string.Format("Broken database detected! ([{0}].[{1}] {2}) has an invalid length of {3}, must be between {4} and {5}", (object) table, (object) column, (object) type.ToString(), (object) maxLength, (object) 1, (object) 8000));
          precision = maxLength;
          octetLength = maxLength;
          break;
        case VistaDBType.UniqueIdentifier:
          precision = 36;
          break;
        case VistaDBType.SmallDateTime:
          precision = 16;
          scale = (short) 0;
          dateTime = (short) -1;
          break;
        case VistaDBType.Timestamp:
          precision = 8;
          octetLength = maxLength;
          break;
      }
    }

    private static Statement ParseProcedure(Database db, string body, out List<SQLParser.VariableDeclaration> variables)
    {
      return (Statement) ((LocalSQLConnection) VistaDBContext.SQLChannel.CurrentConnection).CreateStoredProcedureStatement((Statement) null, body, out variables);
    }

    private static SelectStatement ParseView(Database db, string body, out List<string> columnNames)
    {
      LocalSQLConnection currentConnection = (LocalSQLConnection) VistaDBContext.SQLChannel.CurrentConnection;
      SelectStatement selectStatement = (SelectStatement) null;
      columnNames = (List<string>) null;
      Statement statement = (Statement) currentConnection.CreateBatchStatement(body, 0L).SubQuery(0);
      CreateViewStatement createViewStatement = statement as CreateViewStatement;
      if (createViewStatement != null)
      {
        int num = (int) statement.PrepareQuery();
        selectStatement = createViewStatement.SelectStatement;
        columnNames = createViewStatement.ColumnNames;
        createViewStatement.DropTemporaryTables();
      }
      return selectStatement;
    }

    [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillTableSchema")]
    public static IEnumerable TableSchema()
    {
      List<SystemFunctions.TableSchemaEntry> tableSchemaEntryList = new List<SystemFunctions.TableSchemaEntry>();
      Database currentDatabase = SystemFunctions.GetCurrentDatabase();
      string databaseName = SystemFunctions.GetDatabaseName(currentDatabase);
      foreach (string key in (IEnumerable<string>) currentDatabase.GetTableIdMap().Keys)
      {
        IVistaDBTableSchema tableSchema = (IVistaDBTableSchema) currentDatabase.GetTableSchema(key, false);
        tableSchemaEntryList.Add(new SystemFunctions.TableSchemaEntry(databaseName, tableSchema));
      }
      foreach (Database.ViewList.View schema in (IEnumerable) currentDatabase.LoadViews().Values)
        tableSchemaEntryList.Add(new SystemFunctions.TableSchemaEntry(databaseName, schema));
      return (IEnumerable) tableSchemaEntryList;
    }

    public static void FillTableSchema(object source, VistaDBString table_catalog, VistaDBString table_schema, VistaDBString table_name, VistaDBString table_type)
    {
      SystemFunctions.TableSchemaEntry tableSchemaEntry = (SystemFunctions.TableSchemaEntry) source;
      table_catalog.Value = (object) tableSchemaEntry.Catalog;
      table_schema.Value = (object) tableSchemaEntry.Owner;
      table_name.Value = (object) tableSchemaEntry.Name;
      table_type.Value = (object) tableSchemaEntry.TableType;
    }

    [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.EF_Tables_FillRow")]
    public static IEnumerable EF_Tables_Query()
    {
      Database currentDatabase = SystemFunctions.GetCurrentDatabase();
      string databaseName = SystemFunctions.GetDatabaseName(currentDatabase);
      List<IVistaDBValue[]> vistaDbValueArrayList = new List<IVistaDBValue[]>(10);
      foreach (string key in (IEnumerable<string>) currentDatabase.GetTableIdMap().Keys)
      {
        IVistaDBValue[] vistaDbValueArray = new IVistaDBValue[4]{ (IVistaDBValue) new VistaDBString("[dbo][" + key + "]"), (IVistaDBValue) new VistaDBString(databaseName), (IVistaDBValue) new VistaDBString("dbo"), (IVistaDBValue) new VistaDBString(key) };
        vistaDbValueArrayList.Add(vistaDbValueArray);
      }
      return (IEnumerable) vistaDbValueArrayList;
    }

    public static void EF_Tables_FillRow(object entry, out VistaDBString Id, out VistaDBString CatalogName, out VistaDBString SchemaName, out VistaDBString Name)
    {
      IVistaDBValue[] vistaDbValueArray = (IVistaDBValue[]) entry;
      Id = (VistaDBString) vistaDbValueArray[0];
      CatalogName = (VistaDBString) vistaDbValueArray[1];
      SchemaName = (VistaDBString) vistaDbValueArray[2];
      Name = (VistaDBString) vistaDbValueArray[3];
    }

    private static void FillColumnInfo(IVistaDBColumnAttributes column, IVistaDBValue[] row, int offset)
    {
      SystemFunctions.FillColumnInfo(column.Name, column.RowIndex, column.AllowNull, column.Type, column.MaxLength, row, offset);
    }

    private static void FillColumnInfo(string name, int index, bool allowNull, VistaDBType type, int maxLength, IVistaDBValue[] row, int offset)
    {
      row[offset] = (IVistaDBValue) new VistaDBString(name);
      row[offset + 1] = (IVistaDBValue) new VistaDBInt32(index);
      row[offset + 2] = (IVistaDBValue) new VistaDBBoolean(allowNull);
      string lower = type.ToString().ToLower(CultureInfo.CurrentCulture);
      switch (type)
      {
        case VistaDBType.VarChar:
        case VistaDBType.NVarChar:
        case VistaDBType.VarBinary:
          row[offset + 3] = maxLength < 1 || maxLength > 8000 ? (IVistaDBValue) new VistaDBString(lower + "(max)") : (IVistaDBValue) new VistaDBString(lower);
          break;
        default:
          row[offset + 3] = (IVistaDBValue) new VistaDBString(lower);
          break;
      }
      switch (type)
      {
        case VistaDBType.Char:
        case VistaDBType.VarChar:
          row[offset + 4] = (IVistaDBValue) new VistaDBInt32(maxLength);
          break;
        case VistaDBType.NChar:
        case VistaDBType.NVarChar:
          row[offset + 4] = (IVistaDBValue) new VistaDBInt32(maxLength);
          break;
        case VistaDBType.Text:
          row[offset + 4] = (IVistaDBValue) new VistaDBInt32(maxLength);
          break;
        case VistaDBType.NText:
          row[offset + 4] = (IVistaDBValue) new VistaDBInt32(maxLength);
          break;
        case VistaDBType.Image:
          row[offset + 4] = (IVistaDBValue) new VistaDBInt32(maxLength);
          break;
        case VistaDBType.VarBinary:
          row[offset + 4] = (IVistaDBValue) new VistaDBInt32(maxLength);
          break;
        case VistaDBType.Timestamp:
          row[offset + 4] = (IVistaDBValue) new VistaDBInt32(maxLength);
          break;
        default:
          row[offset + 4] = (IVistaDBValue) new VistaDBInt32();
          break;
      }
      switch (type)
      {
        case VistaDBType.TinyInt:
          row[offset + 5] = (IVistaDBValue) new VistaDBInt32(3);
          row[offset + 6] = (IVistaDBValue) new VistaDBInt32();
          row[offset + 7] = (IVistaDBValue) new VistaDBInt32(0);
          break;
        case VistaDBType.SmallInt:
          row[offset + 5] = (IVistaDBValue) new VistaDBInt32(5);
          row[offset + 6] = (IVistaDBValue) new VistaDBInt32();
          row[offset + 7] = (IVistaDBValue) new VistaDBInt32(0);
          break;
        case VistaDBType.Int:
          row[offset + 5] = (IVistaDBValue) new VistaDBInt32(10);
          row[offset + 6] = (IVistaDBValue) new VistaDBInt32();
          row[offset + 7] = (IVistaDBValue) new VistaDBInt32(0);
          break;
        case VistaDBType.BigInt:
          row[offset + 5] = (IVistaDBValue) new VistaDBInt32(19);
          row[offset + 6] = (IVistaDBValue) new VistaDBInt32();
          row[offset + 7] = (IVistaDBValue) new VistaDBInt32(0);
          break;
        case VistaDBType.Real:
          row[offset + 5] = (IVistaDBValue) new VistaDBInt32(7);
          row[offset + 6] = (IVistaDBValue) new VistaDBInt32();
          row[offset + 7] = (IVistaDBValue) new VistaDBInt32();
          break;
        case VistaDBType.Float:
          row[offset + 5] = (IVistaDBValue) new VistaDBInt32(15);
          row[offset + 6] = (IVistaDBValue) new VistaDBInt32();
          row[offset + 7] = (IVistaDBValue) new VistaDBInt32();
          break;
        case VistaDBType.Decimal:
          row[offset + 5] = (IVistaDBValue) new VistaDBInt32(18);
          row[offset + 6] = (IVistaDBValue) new VistaDBInt32();
          row[offset + 7] = (IVistaDBValue) new VistaDBInt32(0);
          break;
        case VistaDBType.Money:
          row[offset + 5] = (IVistaDBValue) new VistaDBInt32(19);
          row[offset + 6] = (IVistaDBValue) new VistaDBInt32();
          row[offset + 7] = (IVistaDBValue) new VistaDBInt32(4);
          break;
        case VistaDBType.SmallMoney:
          row[offset + 5] = (IVistaDBValue) new VistaDBInt32(10);
          row[offset + 6] = (IVistaDBValue) new VistaDBInt32();
          row[offset + 7] = (IVistaDBValue) new VistaDBInt32(4);
          break;
        case VistaDBType.Bit:
          row[offset + 5] = (IVistaDBValue) new VistaDBInt32(1);
          row[offset + 6] = (IVistaDBValue) new VistaDBInt32();
          row[offset + 7] = (IVistaDBValue) new VistaDBInt32();
          break;
        case VistaDBType.DateTime:
          row[offset + 5] = (IVistaDBValue) new VistaDBInt32(23);
          row[offset + 6] = (IVistaDBValue) new VistaDBInt32(3);
          row[offset + 7] = (IVistaDBValue) new VistaDBInt32(3);
          break;
        case VistaDBType.UniqueIdentifier:
          row[offset + 5] = (IVistaDBValue) new VistaDBInt32(36);
          row[offset + 6] = (IVistaDBValue) new VistaDBInt32();
          row[offset + 7] = (IVistaDBValue) new VistaDBInt32();
          break;
        case VistaDBType.SmallDateTime:
          row[offset + 5] = (IVistaDBValue) new VistaDBInt32(16);
          row[offset + 6] = (IVistaDBValue) new VistaDBInt32();
          row[offset + 7] = (IVistaDBValue) new VistaDBInt32(0);
          break;
        case VistaDBType.Timestamp:
          row[offset + 5] = (IVistaDBValue) new VistaDBInt32(8);
          row[offset + 6] = (IVistaDBValue) new VistaDBInt32();
          row[offset + 7] = (IVistaDBValue) new VistaDBInt32();
          break;
        default:
          row[offset + 5] = (IVistaDBValue) new VistaDBInt32();
          row[offset + 6] = (IVistaDBValue) new VistaDBInt32();
          row[offset + 7] = (IVistaDBValue) new VistaDBInt32();
          break;
      }
      row[offset + 8] = (IVistaDBValue) new VistaDBString();
      row[offset + 9] = (IVistaDBValue) new VistaDBString();
      row[offset + 10] = (IVistaDBValue) new VistaDBString();
      row[offset + 11] = (IVistaDBValue) new VistaDBString();
      row[offset + 12] = (IVistaDBValue) new VistaDBString();
      row[offset + 13] = (IVistaDBValue) new VistaDBString();
    }

    [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.EF_TableColumns_FillRow")]
    public static IEnumerable EF_TableColumns_Query()
    {
      Database currentDatabase = SystemFunctions.GetCurrentDatabase();
      SystemFunctions.GetDatabaseName(currentDatabase);
      List<IVistaDBValue[]> vistaDbValueArrayList = new List<IVistaDBValue[]>();
      foreach (string key in (IEnumerable<string>) currentDatabase.GetTableIdMap().Keys)
      {
        using (IVistaDBTableSchema tableSchema = (IVistaDBTableSchema) currentDatabase.GetTableSchema(key, false))
        {
          foreach (IVistaDBColumnAttributes column in (IEnumerable<IVistaDBColumnAttributes>) tableSchema)
          {
            IVistaDBValue[] row = new IVistaDBValue[20];
            row[0] = (IVistaDBValue) new VistaDBString("[dbo][" + key + "][" + column.Name + "]");
            row[1] = (IVistaDBValue) new VistaDBString("[dbo][" + key + "]");
            SystemFunctions.FillColumnInfo(column, row, 2);
            row[16] = (IVistaDBValue) new VistaDBBoolean(false);
            row[17] = tableSchema.Identities[column.Name] == null ? (IVistaDBValue) new VistaDBBoolean(false) : (IVistaDBValue) new VistaDBBoolean(true);
            row[18] = column.Type != VistaDBType.Timestamp ? (IVistaDBValue) new VistaDBBoolean(false) : (IVistaDBValue) new VistaDBBoolean(true);
            IVistaDBDefaultValueInformation defaultValue = tableSchema.DefaultValues[column.Name];
            row[19] = defaultValue != null ? (IVistaDBValue) new VistaDBString(defaultValue.Expression) : (IVistaDBValue) new VistaDBString();
            vistaDbValueArrayList.Add(row);
          }
        }
      }
      return (IEnumerable) vistaDbValueArrayList;
    }

    public static void EF_TableColumns_FillRow(object entry, out VistaDBString Id, out VistaDBString ParentId, out VistaDBString Name, out VistaDBInt32 Ordinal, out VistaDBBoolean IsNullable, out VistaDBString TypeName, out VistaDBInt32 MaxLength, out VistaDBInt32 Precision, out VistaDBInt32 DateTimePrecision, out VistaDBInt32 Scale, out VistaDBString CollationCatalog, out VistaDBString CollationSchema, out VistaDBString CollationName, out VistaDBString CharacterSetCatalog, out VistaDBString CharacterSetSchema, out VistaDBString CharacterSetName, out VistaDBBoolean IsMultiSet, out VistaDBBoolean IsIdentity, out VistaDBBoolean IsStoreGenerated, out VistaDBString Default)
    {
      IVistaDBValue[] vistaDbValueArray = (IVistaDBValue[]) entry;
      Id = (VistaDBString) vistaDbValueArray[0];
      ParentId = (VistaDBString) vistaDbValueArray[1];
      Name = (VistaDBString) vistaDbValueArray[2];
      Ordinal = (VistaDBInt32) vistaDbValueArray[3];
      IsNullable = (VistaDBBoolean) vistaDbValueArray[4];
      TypeName = (VistaDBString) vistaDbValueArray[5];
      MaxLength = (VistaDBInt32) vistaDbValueArray[6];
      Precision = (VistaDBInt32) vistaDbValueArray[7];
      DateTimePrecision = (VistaDBInt32) vistaDbValueArray[8];
      Scale = (VistaDBInt32) vistaDbValueArray[9];
      CollationCatalog = (VistaDBString) vistaDbValueArray[10];
      CollationSchema = (VistaDBString) vistaDbValueArray[11];
      CollationName = (VistaDBString) vistaDbValueArray[12];
      CharacterSetCatalog = (VistaDBString) vistaDbValueArray[13];
      CharacterSetSchema = (VistaDBString) vistaDbValueArray[14];
      CharacterSetName = (VistaDBString) vistaDbValueArray[15];
      IsMultiSet = (VistaDBBoolean) vistaDbValueArray[16];
      IsIdentity = (VistaDBBoolean) vistaDbValueArray[17];
      IsStoreGenerated = (VistaDBBoolean) vistaDbValueArray[18];
      Default = (VistaDBString) vistaDbValueArray[19];
    }

    [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillColumnSchema")]
    public static IEnumerable ColumnSchema()
    {
      List<SystemFunctions.ColumnSchemaEntry> columnSchemaEntryList = new List<SystemFunctions.ColumnSchemaEntry>();
      Database currentDatabase = SystemFunctions.GetCurrentDatabase();
      string databaseName = SystemFunctions.GetDatabaseName(currentDatabase);
      foreach (string key in (IEnumerable<string>) currentDatabase.GetTableIdMap().Keys)
      {
        IVistaDBTableSchema tableSchema = (IVistaDBTableSchema) currentDatabase.GetTableSchema(key, false);
        foreach (IVistaDBColumnAttributes schema in (IEnumerable<IVistaDBColumnAttributes>) tableSchema)
        {
          IVistaDBDefaultValueInformation defaultValue = tableSchema.DefaultValues[schema.Name];
          IVistaDBIdentityInformation identity = tableSchema.Identities[schema.Name];
          columnSchemaEntryList.Add(new SystemFunctions.ColumnSchemaEntry(databaseName, tableSchema, schema, defaultValue, identity));
        }
      }
      foreach (DataRow row in (InternalDataCollectionBase) new VistaDBConnection(VistaDBContext.DDAChannel.CurrentDatabase).GetSchema("VIEWCOLUMNS").Rows)
        columnSchemaEntryList.Add(new SystemFunctions.ColumnSchemaEntry(databaseName, row));
      return (IEnumerable) columnSchemaEntryList;
    }

    public static void FillColumnSchema(object source, VistaDBString table_catalog, VistaDBString table_schema, VistaDBString table_name, VistaDBString column_name, VistaDBInt32 ordinal_position, VistaDBString column_default, VistaDBString is_nullable, VistaDBString data_type, VistaDBInt32 character_maximum_length, VistaDBInt32 character_octet_length, VistaDBByte numeric_precision, VistaDBInt16 numeric_precision_radix, VistaDBInt16 numeric_scale, VistaDBInt16 datetime_precision, VistaDBString character_set_catalog, VistaDBString character_set_schema, VistaDBString character_set_name, VistaDBString collation_catalog, VistaDBString collation_schema, VistaDBString collation_name, VistaDBString domain_catalog, VistaDBString domain_schema, VistaDBString domain_name, VistaDBBoolean is_identity, VistaDBBoolean is_storegenerated)
    {
      SystemFunctions.ColumnSchemaEntry columnSchemaEntry = (SystemFunctions.ColumnSchemaEntry) source;
      table_catalog.Value = (object) columnSchemaEntry.Catalog;
      table_schema.Value = (object) columnSchemaEntry.Owner;
      table_name.Value = (object) columnSchemaEntry.Table;
      column_name.Value = (object) columnSchemaEntry.Name;
      ordinal_position.Value = (object) columnSchemaEntry.Index;
      column_default.Value = (object) columnSchemaEntry.Default;
      is_nullable.Value = columnSchemaEntry.Nullable ? (object) "YES" : (object) "NO";
      data_type.Value = (object) columnSchemaEntry.DataType.ToString().ToLower();
      is_identity.Value = (object) columnSchemaEntry.IsIdentity;
      character_maximum_length.Value = (object) null;
      character_octet_length.Value = (object) null;
      numeric_precision.Value = (object) null;
      numeric_precision_radix.Value = (object) null;
      numeric_scale.Value = (object) null;
      datetime_precision.Value = (object) null;
      if (columnSchemaEntry.DateTimeSub != (short) 0)
        datetime_precision.Value = (object) columnSchemaEntry.DateTimeSub;
      else if (columnSchemaEntry.Radix != (short) 0)
      {
        numeric_precision.Value = (object) (byte) columnSchemaEntry.Precision;
        numeric_precision_radix.Value = (object) columnSchemaEntry.Radix;
        numeric_scale.Value = (object) columnSchemaEntry.Scale;
      }
      else if (columnSchemaEntry.MaxLength != 0)
      {
        character_maximum_length.Value = (object) columnSchemaEntry.Precision;
        character_octet_length.Value = (object) columnSchemaEntry.OctetLength;
      }
      character_set_catalog.Value = (object) null;
      character_set_schema.Value = (object) null;
      character_set_name.Value = (object) null;
      collation_catalog.Value = (object) null;
      collation_schema.Value = (object) null;
      collation_name.Value = (object) null;
      domain_catalog.Value = (object) null;
      domain_schema.Value = (object) null;
      domain_name.Value = (object) null;
    }

    [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.EF_Views_FillRow")]
    public static IEnumerable EF_Views_Query()
    {
      Database currentDatabase = SystemFunctions.GetCurrentDatabase();
      string databaseName = SystemFunctions.GetDatabaseName(currentDatabase);
      List<IVistaDBValue[]> vistaDbValueArrayList = new List<IVistaDBValue[]>();
      foreach (Database.ViewList.View view in (IEnumerable) currentDatabase.LoadViews().Values)
      {
        IVistaDBValue[] vistaDbValueArray = new IVistaDBValue[6]{ (IVistaDBValue) new VistaDBString("[dbo][" + view.Name + "]"), (IVistaDBValue) new VistaDBString(databaseName), (IVistaDBValue) new VistaDBString("dbo"), (IVistaDBValue) new VistaDBString(view.Name), (IVistaDBValue) new VistaDBString(view.Expression), (IVistaDBValue) new VistaDBBoolean(false) };
        vistaDbValueArrayList.Add(vistaDbValueArray);
      }
      return (IEnumerable) vistaDbValueArrayList;
    }

    public static void EF_Views_FillRow(object entry, out VistaDBString Id, out VistaDBString CatalogName, out VistaDBString SchemaName, out VistaDBString Name, out VistaDBString ViewDefinition, out VistaDBBoolean IsUpdatable)
    {
      IVistaDBValue[] vistaDbValueArray = (IVistaDBValue[]) entry;
      Id = (VistaDBString) vistaDbValueArray[0];
      CatalogName = (VistaDBString) vistaDbValueArray[1];
      SchemaName = (VistaDBString) vistaDbValueArray[2];
      Name = (VistaDBString) vistaDbValueArray[3];
      ViewDefinition = (VistaDBString) vistaDbValueArray[4];
      IsUpdatable = (VistaDBBoolean) vistaDbValueArray[5];
    }

    [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.EF_ViewColumns_FillRow")]
    public static IEnumerable EF_ViewColumns_Query()
    {
      Database currentDatabase = SystemFunctions.GetCurrentDatabase();
      SystemFunctions.GetDatabaseName(currentDatabase);
      LocalSQLConnection currentConnection = (LocalSQLConnection) VistaDBContext.SQLChannel.CurrentConnection;
      List<IVistaDBValue[]> vistaDbValueArrayList = new List<IVistaDBValue[]>();
      foreach (Database.ViewList.View view in (IEnumerable) currentDatabase.LoadViews().Values)
      {
        CreateViewStatement createViewStatement = (CreateViewStatement) null;
        try
        {
          Statement statement = (Statement) currentConnection.CreateBatchStatement(view.Expression, 0L).SubQuery(0);
          createViewStatement = statement as CreateViewStatement;
          if (createViewStatement != null)
          {
            int num1 = (int) statement.PrepareQuery();
            SelectStatement selectStatement = ((CreateViewStatement) statement).SelectStatement;
            int num2 = 0;
            for (int columnCount = selectStatement.ColumnCount; num2 < columnCount; ++num2)
            {
              IVistaDBValue[] row = new IVistaDBValue[20];
              string aliasName = selectStatement.GetAliasName(num2);
              row[0] = (IVistaDBValue) new VistaDBString("[dbo][" + view.Name + "][" + aliasName + "]");
              row[1] = (IVistaDBValue) new VistaDBString("[dbo][" + view.Name + "]");
              VistaDBType columnVistaDbType = selectStatement.GetColumnVistaDBType(num2);
              SystemFunctions.FillColumnInfo(aliasName, num2, selectStatement.GetIsAllowNull(num2), columnVistaDbType, selectStatement.GetWidth(num2), row, 2);
              row[16] = (IVistaDBValue) new VistaDBBoolean(false);
              row[17] = (IVistaDBValue) new VistaDBBoolean(selectStatement.GetIsAutoIncrement(num2));
              row[18] = columnVistaDbType != VistaDBType.Timestamp ? (IVistaDBValue) new VistaDBBoolean(false) : (IVistaDBValue) new VistaDBBoolean(true);
              bool useInUpdate;
              row[19] = (IVistaDBValue) new VistaDBString(selectStatement.GetDefaultValue(num2, out useInUpdate));
              vistaDbValueArrayList.Add(row);
            }
            createViewStatement.DropTemporaryTables();
          }
        }
        catch (Exception ex)
        {
        }
        finally
        {
          createViewStatement?.DropTemporaryTables();
        }
      }
      return (IEnumerable) vistaDbValueArrayList;
    }

    public static void EF_ViewColumns_FillRow(object entry, out VistaDBString Id, out VistaDBString ParentId, out VistaDBString Name, out VistaDBInt32 Ordinal, out VistaDBBoolean IsNullable, out VistaDBString TypeName, out VistaDBInt32 MaxLength, out VistaDBInt32 Precision, out VistaDBInt32 DateTimePrecision, out VistaDBInt32 Scale, out VistaDBString CollationCatalog, out VistaDBString CollationSchema, out VistaDBString CollationName, out VistaDBString CharacterSetCatalog, out VistaDBString CharacterSetSchema, out VistaDBString CharacterSetName, out VistaDBBoolean IsMultiSet, out VistaDBBoolean IsIdentity, out VistaDBBoolean IsStoreGenerated, out VistaDBString Default)
    {
      IVistaDBValue[] vistaDbValueArray = (IVistaDBValue[]) entry;
      Id = (VistaDBString) vistaDbValueArray[0];
      ParentId = (VistaDBString) vistaDbValueArray[1];
      Name = (VistaDBString) vistaDbValueArray[2];
      Ordinal = (VistaDBInt32) vistaDbValueArray[3];
      IsNullable = (VistaDBBoolean) vistaDbValueArray[4];
      TypeName = (VistaDBString) vistaDbValueArray[5];
      MaxLength = (VistaDBInt32) vistaDbValueArray[6];
      Precision = (VistaDBInt32) vistaDbValueArray[7];
      DateTimePrecision = (VistaDBInt32) vistaDbValueArray[8];
      Scale = (VistaDBInt32) vistaDbValueArray[9];
      CollationCatalog = (VistaDBString) vistaDbValueArray[10];
      CollationSchema = (VistaDBString) vistaDbValueArray[11];
      CollationName = (VistaDBString) vistaDbValueArray[12];
      CharacterSetCatalog = (VistaDBString) vistaDbValueArray[13];
      CharacterSetSchema = (VistaDBString) vistaDbValueArray[14];
      CharacterSetName = (VistaDBString) vistaDbValueArray[15];
      IsMultiSet = (VistaDBBoolean) vistaDbValueArray[16];
      IsIdentity = (VistaDBBoolean) vistaDbValueArray[17];
      IsStoreGenerated = (VistaDBBoolean) vistaDbValueArray[18];
      Default = (VistaDBString) vistaDbValueArray[19];
    }

    [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillViewSchema")]
    public static IEnumerable ViewSchema()
    {
      List<SystemFunctions.ViewSchemaEntry> viewSchemaEntryList = new List<SystemFunctions.ViewSchemaEntry>();
      Database currentDatabase = SystemFunctions.GetCurrentDatabase();
      string databaseName = SystemFunctions.GetDatabaseName(currentDatabase);
      foreach (Database.ViewList.View schema in (IEnumerable) currentDatabase.LoadViews().Values)
        viewSchemaEntryList.Add(new SystemFunctions.ViewSchemaEntry(databaseName, schema));
      return (IEnumerable) viewSchemaEntryList;
    }

    public static void FillViewSchema(object source, VistaDBString table_catalog, VistaDBString table_schema, VistaDBString table_name, VistaDBString view_definition, VistaDBString check_option, VistaDBString is_updatable)
    {
      SystemFunctions.ViewSchemaEntry viewSchemaEntry = (SystemFunctions.ViewSchemaEntry) source;
      table_catalog.Value = (object) viewSchemaEntry.Catalog;
      table_schema.Value = (object) viewSchemaEntry.Owner;
      table_name.Value = (object) viewSchemaEntry.Name;
      view_definition.Value = (object) viewSchemaEntry.Expression;
      check_option.Value = (object) "NONE";
      is_updatable.Value = (object) "NO";
    }

    [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillViewColumnSchema")]
    public static IEnumerable ViewColumnSchema()
    {
      List<SystemFunctions.ViewColumnDataRow> viewColumnDataRowList = new List<SystemFunctions.ViewColumnDataRow>();
      VistaDBConnection currentConnection = SystemFunctions.GetCurrentConnection();
      string databaseName = SystemFunctions.GetDatabaseName(currentConnection.Database);
      foreach (DataRow row in (InternalDataCollectionBase) currentConnection.GetSchema("VIEWCOLUMNS").Rows)
        viewColumnDataRowList.Add(new SystemFunctions.ViewColumnDataRow(databaseName, row));
      return (IEnumerable) viewColumnDataRowList;
    }

    public static void FillViewColumnSchema(object source, VistaDBString view_catalog, VistaDBString view_schema, VistaDBString view_name, VistaDBString table_catalog, VistaDBString table_schema, VistaDBString table_name, VistaDBString column_name)
    {
      SystemFunctions.ViewColumnDataRow viewColumnDataRow = source as SystemFunctions.ViewColumnDataRow;
      view_catalog.Value = (object) viewColumnDataRow.ViewCatalog;
      view_schema.Value = (object) viewColumnDataRow.ViewSchema;
      view_name.Value = (object) viewColumnDataRow.ViewName;
      table_catalog.Value = (object) viewColumnDataRow.TableCatalog;
      table_schema.Value = (object) viewColumnDataRow.TableSchema;
      table_name.Value = (object) viewColumnDataRow.TableName;
      column_name.Value = (object) viewColumnDataRow.Name;
    }

    [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillViewTableSchema")]
    public static IEnumerable ViewTableSchema()
    {
      return (IEnumerable) new List<object>();
    }

    public static void FillViewTableSchema(object source, VistaDBString view_catalog, VistaDBString view_schema, VistaDBString view_name, VistaDBString table_catalog, VistaDBString table_schema, VistaDBString table_name)
    {
    }

    [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.EF_Constraints_FillRow")]
    public static IEnumerable EF_Constraints_Query()
    {
      Database currentDatabase = SystemFunctions.GetCurrentDatabase();
      SystemFunctions.GetDatabaseName(currentDatabase);
      LocalSQLConnection currentConnection = (LocalSQLConnection) VistaDBContext.SQLChannel.CurrentConnection;
      List<IVistaDBValue[]> vistaDbValueArrayList = new List<IVistaDBValue[]>(10);
      foreach (KeyValuePair<ulong, string> tableId in currentDatabase.GetTableIdMap())
      {
        IVistaDBIndexCollection indexes = (IVistaDBIndexCollection) new Table.TableSchema.IndexCollection();
        currentDatabase.GetIndexes(tableId.Key, indexes);
        foreach (IVistaDBIndexInformation indexInformation in (IEnumerable<IVistaDBIndexInformation>) indexes.Values)
        {
          if (indexInformation.Unique || indexInformation.Primary || indexInformation.FKConstraint)
          {
            IVistaDBValue[] vistaDbValueArray = new IVistaDBValue[6]{ (IVistaDBValue) new VistaDBString("[dbo][" + tableId.Value + "][" + indexInformation.Name + "]"), (IVistaDBValue) new VistaDBString("[dbo][" + tableId.Value + "]"), (IVistaDBValue) new VistaDBString(indexInformation.Name), null, null, null };
            string val = (string) null;
            if (indexInformation.Primary)
              val = "PRIMARY KEY";
            else if (indexInformation.Unique)
              val = "UNIQUE";
            else if (indexInformation.FKConstraint)
              val = "FOREIGN KEY";
            vistaDbValueArray[3] = (IVistaDBValue) new VistaDBString(val);
            vistaDbValueArray[4] = (IVistaDBValue) new VistaDBBoolean(false);
            vistaDbValueArray[5] = (IVistaDBValue) new VistaDBBoolean(false);
            vistaDbValueArrayList.Add(vistaDbValueArray);
          }
        }
      }
      return (IEnumerable) vistaDbValueArrayList;
    }

    public static void EF_Constraints_FillRow(object entry, out VistaDBString Id, out VistaDBString ParentId, out VistaDBString Name, out VistaDBString ConstraintType, out VistaDBBoolean IsDeferrable, out VistaDBBoolean IsInitiallyDeferred)
    {
      IVistaDBValue[] vistaDbValueArray = (IVistaDBValue[]) entry;
      Id = (VistaDBString) vistaDbValueArray[0];
      ParentId = (VistaDBString) vistaDbValueArray[1];
      Name = (VistaDBString) vistaDbValueArray[2];
      ConstraintType = (VistaDBString) vistaDbValueArray[3];
      IsDeferrable = (VistaDBBoolean) vistaDbValueArray[4];
      IsInitiallyDeferred = (VistaDBBoolean) vistaDbValueArray[5];
    }

    [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillTableConstraintSchema")]
    public static IEnumerable TableConstraintSchema()
    {
      List<SystemFunctions.RelationshipSchemaEntry> relationshipSchemaEntryList = new List<SystemFunctions.RelationshipSchemaEntry>();
      Database currentDatabase = SystemFunctions.GetCurrentDatabase();
      string databaseName = SystemFunctions.GetDatabaseName(currentDatabase);
      foreach (string key in (IEnumerable<string>) currentDatabase.GetTableIdMap().Keys)
      {
        IVistaDBTableSchema tableSchema = (IVistaDBTableSchema) currentDatabase.GetTableSchema(key, true);
        foreach (IVistaDBIndexInformation index in (IEnumerable<IVistaDBIndexInformation>) tableSchema.Indexes.Values)
        {
          if (index.Unique || index.Primary || index.FKConstraint)
            relationshipSchemaEntryList.Add(new SystemFunctions.RelationshipSchemaEntry(databaseName, tableSchema.Name, index));
        }
        foreach (IVistaDBConstraintInformation constraint in (IEnumerable<IVistaDBConstraintInformation>) tableSchema.Constraints.Values)
          relationshipSchemaEntryList.Add(new SystemFunctions.RelationshipSchemaEntry(databaseName, tableSchema.Name, constraint));
      }
      return (IEnumerable) relationshipSchemaEntryList;
    }

    public static void FillTableConstraintSchema(object source, VistaDBString constraint_catalog, VistaDBString constraint_schema, VistaDBString constraint_name, VistaDBString table_catalog, VistaDBString table_schema, VistaDBString table_name, VistaDBString constraint_type, VistaDBString is_deferrable, VistaDBString initially_deferred)
    {
      SystemFunctions.RelationshipSchemaEntry relationshipSchemaEntry = (SystemFunctions.RelationshipSchemaEntry) source;
      constraint_catalog.Value = (object) relationshipSchemaEntry.Catalog;
      constraint_schema.Value = (object) relationshipSchemaEntry.Owner;
      constraint_name.Value = (object) relationshipSchemaEntry.Name;
      table_catalog.Value = (object) relationshipSchemaEntry.Catalog;
      table_schema.Value = (object) relationshipSchemaEntry.Owner;
      table_name.Value = (object) relationshipSchemaEntry.Table;
      constraint_type.Value = (object) relationshipSchemaEntry.RelationType;
      is_deferrable.Value = (object) "NO";
      initially_deferred.Value = (object) "NO";
    }

    [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.EF_ForeignKeyConstraints_FillRow")]
    public static IEnumerable EF_ForeignKeyConstraints_Query()
    {
      Database currentDatabase = SystemFunctions.GetCurrentDatabase();
      SystemFunctions.GetDatabaseName(currentDatabase);
      List<IVistaDBValue[]> vistaDbValueArrayList = new List<IVistaDBValue[]>(10);
      foreach (IVistaDBRelationshipInformation relationshipInformation in (IEnumerable<IVistaDBRelationshipInformation>) currentDatabase.GetRelationships().Values)
      {
        IVistaDBValue[] vistaDbValueArray = new IVistaDBValue[3]{ (IVistaDBValue) new VistaDBString("[dbo][" + relationshipInformation.ForeignTable + "][" + relationshipInformation.Name + "]"), (IVistaDBValue) new VistaDBString(SystemFunctions.GetReferentialIntegrity(relationshipInformation.UpdateIntegrity)), (IVistaDBValue) new VistaDBString(SystemFunctions.GetReferentialIntegrity(relationshipInformation.DeleteIntegrity)) };
        vistaDbValueArrayList.Add(vistaDbValueArray);
      }
      return (IEnumerable) vistaDbValueArrayList;
    }

    public static void EF_ForeignKeyConstraints_FillRow(object entry, out VistaDBString Id, out VistaDBString UpdateRule, out VistaDBString DeleteRule)
    {
      IVistaDBValue[] vistaDbValueArray = (IVistaDBValue[]) entry;
      Id = (VistaDBString) vistaDbValueArray[0];
      UpdateRule = (VistaDBString) vistaDbValueArray[1];
      DeleteRule = (VistaDBString) vistaDbValueArray[2];
    }

    [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.EF_ForeignKeys_FillRow")]
    public static IEnumerable EF_ForeignKeys_Query()
    {
      Database currentDatabase = SystemFunctions.GetCurrentDatabase();
      SystemFunctions.GetDatabaseName(currentDatabase);
      List<IVistaDBValue[]> vistaDbValueArrayList = new List<IVistaDBValue[]>(10);
      Dictionary<string, IVistaDBTableSchema> dictionary1 = new Dictionary<string, IVistaDBTableSchema>();
      Dictionary<string, IVistaDBIndexInformation> dictionary2 = new Dictionary<string, IVistaDBIndexInformation>();
      foreach (IVistaDBRelationshipInformation relationshipInformation in (IEnumerable<IVistaDBRelationshipInformation>) currentDatabase.GetRelationships().Values)
      {
        int val = 0;
        IVistaDBTableSchema tableSchema1;
        if (!dictionary1.TryGetValue(relationshipInformation.ForeignTable, out tableSchema1))
        {
          tableSchema1 = (IVistaDBTableSchema) currentDatabase.GetTableSchema(relationshipInformation.ForeignTable, false);
          dictionary1.Add(tableSchema1.Name, tableSchema1);
        }
        IVistaDBTableSchema tableSchema2;
        if (!dictionary1.TryGetValue(relationshipInformation.PrimaryTable, out tableSchema2))
        {
          tableSchema2 = (IVistaDBTableSchema) currentDatabase.GetTableSchema(relationshipInformation.PrimaryTable, false);
          dictionary1.Add(tableSchema2.Name, tableSchema2);
        }
        IVistaDBIndexInformation indexInformation1 = (IVistaDBIndexInformation) null;
        if (!dictionary2.TryGetValue(tableSchema2.Name, out indexInformation1))
        {
          foreach (IVistaDBIndexInformation indexInformation2 in (IEnumerable<IVistaDBIndexInformation>) tableSchema2.Indexes.Values)
          {
            if (indexInformation2.Primary)
            {
              indexInformation1 = indexInformation2;
              break;
            }
          }
          dictionary2.Add(tableSchema2.Name, indexInformation1);
        }
        string[] strArray = relationshipInformation.ForeignKey.Split(';');
        int index = 0;
        for (int length = strArray.Length; index < length; ++index)
        {
          val = tableSchema1[strArray[index]].RowIndex;
          IVistaDBValue[] vistaDbValueArray = new IVistaDBValue[5]{ (IVistaDBValue) new VistaDBString("[dbo][" + relationshipInformation.ForeignTable + "][" + relationshipInformation.Name + "][" + val.ToString() + "]"), (IVistaDBValue) new VistaDBString("[dbo][" + relationshipInformation.PrimaryTable + "][" + tableSchema2[indexInformation1.KeyStructure[index].RowIndex].Name + "]"), (IVistaDBValue) new VistaDBString("[dbo][" + relationshipInformation.ForeignTable + "][" + strArray[index] + "]"), (IVistaDBValue) new VistaDBString("[dbo][" + relationshipInformation.ForeignTable + "][" + relationshipInformation.Name + "]"), (IVistaDBValue) new VistaDBInt32(val) };
          vistaDbValueArrayList.Add(vistaDbValueArray);
        }
      }
      return (IEnumerable) vistaDbValueArrayList;
    }

    public static void EF_ForeignKeys_FillRow(object entry, out VistaDBString Id, out VistaDBString ToColumnId, out VistaDBString FromColumnId, out VistaDBString ConstraintId, out VistaDBInt32 Ordinal)
    {
      IVistaDBValue[] vistaDbValueArray = (IVistaDBValue[]) entry;
      Id = (VistaDBString) vistaDbValueArray[0];
      ToColumnId = (VistaDBString) vistaDbValueArray[1];
      FromColumnId = (VistaDBString) vistaDbValueArray[2];
      ConstraintId = (VistaDBString) vistaDbValueArray[3];
      Ordinal = (VistaDBInt32) vistaDbValueArray[4];
    }

    [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.EF_ConstraintColumns_FillRow")]
    public static IEnumerable EF_ConstraintColumns_Query()
    {
      Database currentDatabase = SystemFunctions.GetCurrentDatabase();
      SystemFunctions.GetDatabaseName(currentDatabase);
      List<IVistaDBValue[]> vistaDbValueArrayList = new List<IVistaDBValue[]>(10);
      foreach (KeyValuePair<ulong, string> tableId in currentDatabase.GetTableIdMap())
      {
        Table.TableSchema.IndexCollection indexCollection = new Table.TableSchema.IndexCollection();
        currentDatabase.GetIndexes(tableId.Key, (IVistaDBIndexCollection) indexCollection);
        foreach (IVistaDBIndexInformation indexInformation in indexCollection.Values)
        {
          if (indexInformation.Unique || indexInformation.Primary || indexInformation.FKConstraint)
          {
            Row row = currentDatabase.AllocateRowsetSchema(tableId.Key, currentDatabase.CreateEmptyRowInstance());
            foreach (IVistaDBKeyColumn vistaDbKeyColumn in indexInformation.KeyStructure)
            {
              IVistaDBValue[] vistaDbValueArray = new IVistaDBValue[2]{ (IVistaDBValue) new VistaDBString("[dbo][" + tableId.Value + "][" + indexInformation.Name + "]"), (IVistaDBValue) new VistaDBString("[dbo][" + tableId.Value + "][" + row[vistaDbKeyColumn.RowIndex].Name + "]") };
              vistaDbValueArrayList.Add(vistaDbValueArray);
            }
          }
        }
      }
      return (IEnumerable) vistaDbValueArrayList;
    }

    public static void EF_ConstraintColumns_FillRow(object entry, out VistaDBString ConstraintId, out VistaDBString ColumnId)
    {
      IVistaDBValue[] vistaDbValueArray = (IVistaDBValue[]) entry;
      ConstraintId = (VistaDBString) vistaDbValueArray[0];
      ColumnId = (VistaDBString) vistaDbValueArray[1];
    }

    private static string GetReferentialIntegrity(VistaDBReferentialIntegrity integrity)
    {
      switch (integrity)
      {
        case VistaDBReferentialIntegrity.Cascade:
          return "CASCADE";
        case VistaDBReferentialIntegrity.SetNull:
          return "SET NULL";
        case VistaDBReferentialIntegrity.SetDefault:
          return "SET DEFAULT";
        default:
          return "NONE";
      }
    }

    [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillReferentialConstraintSchema")]
    public static IEnumerable ReferentialConstraintSchema()
    {
      List<SystemFunctions.RelationshipSchemaEntry> relationshipSchemaEntryList = new List<SystemFunctions.RelationshipSchemaEntry>();
      Database currentDatabase = SystemFunctions.GetCurrentDatabase();
      string databaseName = SystemFunctions.GetDatabaseName(currentDatabase);
      foreach (string key in (IEnumerable<string>) currentDatabase.GetTableIdMap().Keys)
      {
        IVistaDBTableSchema tableSchema = (IVistaDBTableSchema) currentDatabase.GetTableSchema(key, true);
        foreach (IVistaDBIndexInformation indexInformation1 in (IEnumerable<IVistaDBIndexInformation>) tableSchema.Indexes.Values)
        {
          if (indexInformation1.FKConstraint)
          {
            IVistaDBRelationshipInformation foreignKey1 = tableSchema.ForeignKeys[indexInformation1.Name];
            IVistaDBTableSchema primaryTable = (IVistaDBTableSchema) currentDatabase.GetTableSchema(foreignKey1.PrimaryTable, true);
            IVistaDBIndexInformation foreign_index = (IVistaDBIndexInformation) null;
            foreach (IVistaDBIndexInformation indexInformation2 in (IEnumerable<IVistaDBIndexInformation>) primaryTable.Indexes.Values)
            {
              if (indexInformation2.Primary)
              {
                bool flag = false;
                string foreignKey2 = foreignKey1.ForeignKey;
                char[] chArray = new char[1]{ ';' };
                foreach (string str in foreignKey2.Split(chArray))
                {
                  string foreignKey = str;
                  flag = Array.Exists<IVistaDBKeyColumn>(indexInformation2.KeyStructure, (Predicate<IVistaDBKeyColumn>) (matchKey => string.Compare(primaryTable[matchKey.RowIndex].Name, foreignKey, StringComparison.OrdinalIgnoreCase) == 0));
                  if (flag)
                    break;
                }
                if (flag)
                {
                  foreign_index = indexInformation2;
                  break;
                }
              }
            }
            if (foreign_index != null)
              relationshipSchemaEntryList.Add(new SystemFunctions.RelationshipSchemaEntry(databaseName, foreignKey1, foreign_index));
          }
        }
      }
      return (IEnumerable) relationshipSchemaEntryList;
    }

    public static void FillReferentialConstraintSchema(object source, VistaDBString constraint_catalog, VistaDBString constraint_schema, VistaDBString constraint_name, VistaDBString unique_constraint_catalog, VistaDBString unique_constraint_schema, VistaDBString unique_constraint_name, VistaDBString match_option, VistaDBString update_rule, VistaDBString delete_rule)
    {
      SystemFunctions.RelationshipSchemaEntry relationshipSchemaEntry = (SystemFunctions.RelationshipSchemaEntry) source;
      constraint_catalog.Value = (object) relationshipSchemaEntry.Catalog;
      constraint_schema.Value = (object) relationshipSchemaEntry.Owner;
      constraint_name.Value = (object) relationshipSchemaEntry.Name;
      unique_constraint_catalog.Value = (object) relationshipSchemaEntry.Catalog;
      unique_constraint_schema.Value = (object) relationshipSchemaEntry.Owner;
      unique_constraint_name.Value = (object) relationshipSchemaEntry.Expression;
      match_option.Value = (object) "SIMPLE";
      update_rule.Value = (object) relationshipSchemaEntry.UpdateRule;
      delete_rule.Value = (object) relationshipSchemaEntry.DeleteRule;
    }

    [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillKeyColumnUsageSchema")]
    public static IEnumerable KeyColumnUsageSchema()
    {
      List<SystemFunctions.RelationshipSchemaEntry> relationshipSchemaEntryList = new List<SystemFunctions.RelationshipSchemaEntry>();
      Database currentDatabase = SystemFunctions.GetCurrentDatabase();
      string databaseName = SystemFunctions.GetDatabaseName(currentDatabase);
      foreach (string key1 in (IEnumerable<string>) currentDatabase.GetTableIdMap().Keys)
      {
        IVistaDBTableSchema tableSchema1 = (IVistaDBTableSchema) currentDatabase.GetTableSchema(key1, true);
        foreach (IVistaDBIndexInformation index1 in (IEnumerable<IVistaDBIndexInformation>) tableSchema1.Indexes.Values)
        {
          if (index1.Primary || index1.Unique)
          {
            foreach (IVistaDBKeyColumn key2 in index1.KeyStructure)
              relationshipSchemaEntryList.Add(new SystemFunctions.RelationshipSchemaEntry(databaseName, tableSchema1, index1, tableSchema1, key2));
          }
          else if (index1.FKConstraint)
          {
            IVistaDBRelationshipInformation foreignKey = tableSchema1.ForeignKeys[index1.Name];
            IVistaDBTableSchema tableSchema2 = (IVistaDBTableSchema) currentDatabase.GetTableSchema(foreignKey.PrimaryTable, true);
            IVistaDBIndexInformation indexInformation1 = (IVistaDBIndexInformation) null;
            foreach (IVistaDBIndexInformation indexInformation2 in (IEnumerable<IVistaDBIndexInformation>) tableSchema2.Indexes.Values)
            {
              if (indexInformation2.Primary)
              {
                indexInformation1 = indexInformation2;
                break;
              }
            }
            for (int index2 = 0; index2 < index1.KeyStructure.Length; ++index2)
              relationshipSchemaEntryList.Add(new SystemFunctions.RelationshipSchemaEntry(databaseName, tableSchema1, index1, tableSchema2, indexInformation1.KeyStructure[index2]));
          }
        }
      }
      return (IEnumerable) relationshipSchemaEntryList;
    }

    public static void FillKeyColumnUsageSchema(object source, VistaDBString constraint_catalog, VistaDBString constraint_schema, VistaDBString constraint_name, VistaDBString table_catalog, VistaDBString table_schema, VistaDBString table_name, VistaDBString column_name, VistaDBInt32 ordinal_position)
    {
      SystemFunctions.RelationshipSchemaEntry relationshipSchemaEntry = (SystemFunctions.RelationshipSchemaEntry) source;
      constraint_catalog.Value = (object) relationshipSchemaEntry.Catalog;
      constraint_schema.Value = (object) relationshipSchemaEntry.Owner;
      constraint_name.Value = (object) relationshipSchemaEntry.Name;
      table_catalog.Value = (object) relationshipSchemaEntry.Catalog;
      table_schema.Value = (object) relationshipSchemaEntry.Owner;
      table_name.Value = (object) relationshipSchemaEntry.Table;
      column_name.Value = (object) relationshipSchemaEntry.Expression;
      ordinal_position.Value = (object) relationshipSchemaEntry.ColumnOrdinal;
    }

    [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillCheckConstraintSchema")]
    public static IEnumerable CheckConstraintSchema()
    {
      List<SystemFunctions.RelationshipSchemaEntry> relationshipSchemaEntryList = new List<SystemFunctions.RelationshipSchemaEntry>();
      Database currentDatabase = SystemFunctions.GetCurrentDatabase();
      string databaseName = SystemFunctions.GetDatabaseName(currentDatabase);
      foreach (string key in (IEnumerable<string>) currentDatabase.GetTableIdMap().Keys)
      {
        IVistaDBTableSchema tableSchema = (IVistaDBTableSchema) currentDatabase.GetTableSchema(key, true);
        foreach (IVistaDBConstraintInformation constraint in (IEnumerable<IVistaDBConstraintInformation>) tableSchema.Constraints.Values)
          relationshipSchemaEntryList.Add(new SystemFunctions.RelationshipSchemaEntry(databaseName, tableSchema.Name, constraint));
      }
      return (IEnumerable) relationshipSchemaEntryList;
    }

    public static void FillCheckConstraintSchema(object source, VistaDBString constraint_catalog, VistaDBString constraint_schema, VistaDBString constraint_name, VistaDBString check_clause)
    {
      SystemFunctions.RelationshipSchemaEntry relationshipSchemaEntry = (SystemFunctions.RelationshipSchemaEntry) source;
      constraint_catalog.Value = (object) relationshipSchemaEntry.Catalog;
      constraint_schema.Value = (object) relationshipSchemaEntry.Owner;
      constraint_name.Value = (object) relationshipSchemaEntry.Name;
      check_clause.Value = (object) relationshipSchemaEntry.Expression;
    }

    [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillForeignKeySchema")]
    public static IEnumerable ForeignKeySchema()
    {
      List<SystemFunctions.ForeignKeySchemaEntry> foreignKeySchemaEntryList = new List<SystemFunctions.ForeignKeySchemaEntry>();
      Database currentDatabase = SystemFunctions.GetCurrentDatabase();
      string databaseName = SystemFunctions.GetDatabaseName(currentDatabase);
      foreach (string key in (IEnumerable<string>) currentDatabase.GetTableIdMap().Keys)
      {
        IVistaDBTableSchema tableSchema = (IVistaDBTableSchema) currentDatabase.GetTableSchema(key, false);
        foreach (IVistaDBIndexInformation index in (IEnumerable<IVistaDBIndexInformation>) tableSchema.Indexes.Values)
        {
          if (index.FKConstraint)
            foreignKeySchemaEntryList.Add(new SystemFunctions.ForeignKeySchemaEntry(databaseName, tableSchema, index, tableSchema.ForeignKeys[index.Name]));
        }
      }
      return (IEnumerable) foreignKeySchemaEntryList;
    }

    public static void FillForeignKeySchema(object source, VistaDBString foreign_key_catalog, VistaDBString foreign_key_schema, VistaDBString foreign_key_name, VistaDBString foreign_key_table, VistaDBString table_name, VistaDBString table_schema, VistaDBInt32 update_referential_action, VistaDBInt32 delete_referential_action)
    {
      SystemFunctions.ForeignKeySchemaEntry foreignKeySchemaEntry = (SystemFunctions.ForeignKeySchemaEntry) source;
      foreign_key_catalog.Value = (object) foreignKeySchemaEntry.Catalog;
      foreign_key_schema.Value = (object) foreignKeySchemaEntry.Owner;
      foreign_key_name.Value = (object) foreignKeySchemaEntry.Name;
      foreign_key_table.Value = (object) foreignKeySchemaEntry.Table;
      table_name.Value = (object) foreignKeySchemaEntry.TargetTable;
      table_schema.Value = (object) foreignKeySchemaEntry.Owner;
      update_referential_action.Value = (object) foreignKeySchemaEntry.Update;
      delete_referential_action.Value = (object) foreignKeySchemaEntry.Delete;
    }

    [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillForeignKeyColumnSchema")]
    public static IEnumerable ForeignKeyColumnSchema()
    {
      List<SystemFunctions.ForeignKeyColumnSchemaEntry> columnSchemaEntryList = new List<SystemFunctions.ForeignKeyColumnSchemaEntry>();
      Database currentDatabase = SystemFunctions.GetCurrentDatabase();
      string databaseName = SystemFunctions.GetDatabaseName(currentDatabase);
      foreach (string key in (IEnumerable<string>) currentDatabase.GetTableIdMap().Keys)
      {
        IVistaDBTableSchema tableSchema1 = (IVistaDBTableSchema) currentDatabase.GetTableSchema(key, false);
        foreach (IVistaDBIndexInformation index in (IEnumerable<IVistaDBIndexInformation>) tableSchema1.Indexes.Values)
        {
          if (index.FKConstraint)
          {
            IVistaDBRelationshipInformation foreignKey = tableSchema1.ForeignKeys[index.Name];
            IVistaDBTableSchema tableSchema2 = (IVistaDBTableSchema) currentDatabase.GetTableSchema(foreignKey.PrimaryTable, true);
            IVistaDBIndexInformation targetKey = (IVistaDBIndexInformation) null;
            foreach (IVistaDBIndexInformation indexInformation in (IEnumerable<IVistaDBIndexInformation>) tableSchema2.Indexes.Values)
            {
              if (indexInformation.Primary)
              {
                targetKey = indexInformation;
                break;
              }
            }
            foreach (IVistaDBKeyColumn column in index.KeyStructure)
              columnSchemaEntryList.Add(new SystemFunctions.ForeignKeyColumnSchemaEntry(databaseName, tableSchema1, index, foreignKey, tableSchema2, column, targetKey));
          }
        }
      }
      return (IEnumerable) columnSchemaEntryList;
    }

    public static void FillForeignKeyColumnSchema(object source, VistaDBString foreign_key_catalog, VistaDBString foreign_key_schema, VistaDBString foreign_key_name, VistaDBString foreign_key_table, VistaDBString foreign_key_column, VistaDBString column_name, VistaDBInt32 ordinal_position)
    {
      SystemFunctions.ForeignKeyColumnSchemaEntry columnSchemaEntry = (SystemFunctions.ForeignKeyColumnSchemaEntry) source;
      foreign_key_catalog.Value = (object) columnSchemaEntry.Catalog;
      foreign_key_schema.Value = (object) columnSchemaEntry.Owner;
      foreign_key_name.Value = (object) columnSchemaEntry.Name;
      foreign_key_table.Value = (object) columnSchemaEntry.Table;
      foreign_key_column.Value = (object) columnSchemaEntry.Key;
      column_name.Value = (object) columnSchemaEntry.Column;
      ordinal_position.Value = (object) columnSchemaEntry.Ordinal;
    }

    [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillIndexSchema")]
    public static IEnumerable IndexSchema()
    {
      List<SystemFunctions.IndexSchemaEntry> indexSchemaEntryList = new List<SystemFunctions.IndexSchemaEntry>();
      Database currentDatabase = SystemFunctions.GetCurrentDatabase();
      string databaseName = SystemFunctions.GetDatabaseName(currentDatabase);
      foreach (string key in (IEnumerable<string>) currentDatabase.GetTableIdMap().Keys)
      {
        IVistaDBTableSchema tableSchema = (IVistaDBTableSchema) currentDatabase.GetTableSchema(key, false);
        foreach (IVistaDBIndexInformation index in (IEnumerable<IVistaDBIndexInformation>) tableSchema.Indexes.Values)
        {
          if (!index.FKConstraint)
            indexSchemaEntryList.Add(new SystemFunctions.IndexSchemaEntry(databaseName, tableSchema, index));
        }
      }
      return (IEnumerable) indexSchemaEntryList;
    }

    public static void FillIndexSchema(object source, VistaDBString index_catalog, VistaDBString index_schema, VistaDBString index_name, VistaDBString table_name, VistaDBBoolean is_primary_key, VistaDBBoolean is_unique)
    {
      SystemFunctions.IndexSchemaEntry indexSchemaEntry = (SystemFunctions.IndexSchemaEntry) source;
      index_catalog.Value = (object) indexSchemaEntry.Catalog;
      index_schema.Value = (object) indexSchemaEntry.Owner;
      index_name.Value = (object) indexSchemaEntry.Name;
      table_name.Value = (object) indexSchemaEntry.Table;
      is_primary_key.Value = (object) indexSchemaEntry.IsPrimary;
      is_unique.Value = (object) indexSchemaEntry.IsUnique;
    }

    [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillIndexColumnSchema")]
    public static IEnumerable IndexColumnSchema()
    {
      List<SystemFunctions.IndexColumnSchemaEntry> columnSchemaEntryList = new List<SystemFunctions.IndexColumnSchemaEntry>();
      Database currentDatabase = SystemFunctions.GetCurrentDatabase();
      string databaseName = SystemFunctions.GetDatabaseName(currentDatabase);
      foreach (string key in (IEnumerable<string>) currentDatabase.GetTableIdMap().Keys)
      {
        IVistaDBTableSchema tableSchema = (IVistaDBTableSchema) currentDatabase.GetTableSchema(key, false);
        foreach (IVistaDBIndexInformation index in (IEnumerable<IVistaDBIndexInformation>) tableSchema.Indexes.Values)
        {
          if (!index.FKConstraint)
          {
            foreach (IVistaDBKeyColumn column in index.KeyStructure)
              columnSchemaEntryList.Add(new SystemFunctions.IndexColumnSchemaEntry(databaseName, tableSchema, index, column));
          }
        }
      }
      return (IEnumerable) columnSchemaEntryList;
    }

    public static void FillIndexColumnSchema(object source, VistaDBString index_catalog, VistaDBString index_schema, VistaDBString index_name, VistaDBString table_name, VistaDBString column_name, VistaDBInt32 key_ordinal)
    {
      SystemFunctions.IndexColumnSchemaEntry columnSchemaEntry = (SystemFunctions.IndexColumnSchemaEntry) source;
      index_catalog.Value = (object) columnSchemaEntry.Catalog;
      index_schema.Value = (object) columnSchemaEntry.Owner;
      index_name.Value = (object) columnSchemaEntry.Index;
      table_name.Value = (object) columnSchemaEntry.Table;
      column_name.Value = (object) columnSchemaEntry.Name;
      key_ordinal.Value = (object) columnSchemaEntry.Ordinal;
    }

    [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillRoutineSchema")]
    public static IEnumerable RoutineSchema()
    {
      List<SystemFunctions.RoutineSchemaEntry> routineSchemaEntryList = new List<SystemFunctions.RoutineSchemaEntry>();
      Database currentDatabase = SystemFunctions.GetCurrentDatabase();
      string databaseName = SystemFunctions.GetDatabaseName(currentDatabase);
      foreach (IStoredProcedureInformation sp in (IEnumerable<IStoredProcedureInformation>) currentDatabase.LoadSqlStoredProcedures().Values)
      {
        List<SQLParser.VariableDeclaration> variables;
        Statement procedure = SystemFunctions.ParseProcedure(currentDatabase, sp.Statement, out variables);
        routineSchemaEntryList.Add(new SystemFunctions.RoutineSchemaEntry(databaseName, sp, procedure, variables));
      }
      return (IEnumerable) routineSchemaEntryList;
    }

    public static void FillRoutineSchema(object source, VistaDBString specific_catalog, VistaDBString specific_schema, VistaDBString specific_name, VistaDBString routine_catalog, VistaDBString routine_schema, VistaDBString routine_name, VistaDBString routine_type, VistaDBString module_catalog, VistaDBString module_schema, VistaDBString module_name, VistaDBString udt_catalog, VistaDBString udt_schema, VistaDBString udt_name, VistaDBString data_type, VistaDBInt32 character_maximum_length, VistaDBInt32 character_octet_length, VistaDBString collation_catalog, VistaDBString collation_schema, VistaDBString collation_name, VistaDBString character_set_catalog, VistaDBString character_set_schema, VistaDBString character_set_name, VistaDBByte numeric_precision, VistaDBInt16 numeric_precision_radix, VistaDBInt16 numeric_scale, VistaDBInt16 datetime_precision, VistaDBString interval_type, VistaDBInt16 interval_precision, VistaDBString type_udt_catalog, VistaDBString type_udt_schema, VistaDBString type_udt_name, VistaDBString scope_catalog, VistaDBString scope_schema, VistaDBString scope_name, VistaDBInt64 maximum_cardinality, VistaDBString dtd_identifier, VistaDBString routine_body, VistaDBString routine_definition, VistaDBString external_name, VistaDBString external_language, VistaDBString parameter_style, VistaDBString is_deterministic, VistaDBString sql_data_access, VistaDBString is_null_call, VistaDBString sql_path, VistaDBString schema_level_routine, VistaDBInt16 max_dynamic_result_set, VistaDBString is_user_defined_cast, VistaDBString is_implicitly_invocable, VistaDBDateTime created, VistaDBDateTime last_altered)
    {
      SystemFunctions.RoutineSchemaEntry routineSchemaEntry = (SystemFunctions.RoutineSchemaEntry) source;
      specific_catalog.Value = (object) routineSchemaEntry.Catalog;
      specific_name.Value = (object) routineSchemaEntry.Name;
      specific_schema.Value = (object) routineSchemaEntry.Owner;
      routine_catalog.Value = (object) routineSchemaEntry.Catalog;
      routine_name.Value = (object) routineSchemaEntry.Name;
      routine_schema.Value = (object) routineSchemaEntry.Owner;
      routine_type.Value = routineSchemaEntry.IsFunction ? (object) "FUNCTION" : (object) "PROCEDURE";
      module_catalog.Value = (object) null;
      module_name.Value = (object) null;
      module_schema.Value = (object) null;
      udt_catalog.Value = (object) null;
      udt_name.Value = (object) null;
      udt_schema.Value = (object) null;
      if (routineSchemaEntry.IsFunction && routineSchemaEntry.IsTable)
        data_type.Value = (object) "table";
      else
        data_type.Value = (object) routineSchemaEntry.DataType.ToString().ToLower();
      character_maximum_length.Value = (object) null;
      character_octet_length.Value = (object) null;
      numeric_precision.Value = (object) null;
      numeric_precision_radix.Value = (object) null;
      numeric_scale.Value = (object) null;
      datetime_precision.Value = (object) null;
      if (routineSchemaEntry.DateTimeSub != (short) 0)
        datetime_precision.Value = (object) routineSchemaEntry.DateTimeSub;
      else if (routineSchemaEntry.Radix != (short) 0)
      {
        numeric_precision.Value = (object) (byte) routineSchemaEntry.Precision;
        numeric_precision_radix.Value = (object) routineSchemaEntry.Radix;
        numeric_scale.Value = (object) routineSchemaEntry.Scale;
      }
      else if (routineSchemaEntry.MaxLength != 0)
      {
        character_maximum_length.Value = (object) routineSchemaEntry.Precision;
        character_octet_length.Value = (object) routineSchemaEntry.OctetLength;
      }
      character_set_catalog.Value = (object) null;
      character_set_schema.Value = (object) null;
      character_set_name.Value = (object) null;
      collation_catalog.Value = (object) null;
      collation_schema.Value = (object) null;
      collation_name.Value = (object) null;
      interval_type.Value = (object) null;
      interval_precision.Value = (object) null;
      type_udt_catalog.Value = (object) null;
      type_udt_schema.Value = (object) null;
      type_udt_name.Value = (object) null;
      scope_catalog.Value = (object) null;
      scope_schema.Value = (object) null;
      scope_name.Value = (object) null;
      maximum_cardinality.Value = (object) null;
      dtd_identifier.Value = (object) null;
      routine_body.Value = routineSchemaEntry.Body == null ? (object) "EXTERNAL" : (object) routineSchemaEntry.Body;
      routine_definition.Value = (object) null;
      external_name.Value = (object) null;
      external_language.Value = (object) null;
      parameter_style.Value = (object) null;
      is_deterministic.Value = !routineSchemaEntry.IsFunction ? (object) "NO" : (object) "YES";
      sql_data_access.Value = !routineSchemaEntry.IsFunction ? (object) "MODIFIED" : (object) "READS";
      is_null_call.Value = (object) "YES";
      sql_path.Value = (object) null;
      schema_level_routine.Value = (object) "YES";
      max_dynamic_result_set.Value = (object) (short) (routineSchemaEntry.IsFunction ? 0 : (int) (short) routineSchemaEntry.MaxResults);
      is_user_defined_cast.Value = (object) "NO";
      is_implicitly_invocable.Value = (object) "NO";
      created.Value = (object) routineSchemaEntry.Created;
      last_altered.Value = (object) routineSchemaEntry.LastAltered;
    }

    [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillParameterSchema")]
    public static IEnumerable ParameterSchema()
    {
      List<SystemFunctions.ParameterSchemaEntry> parameterSchemaEntryList = new List<SystemFunctions.ParameterSchemaEntry>();
      string databaseName = SystemFunctions.GetDatabaseName(SystemFunctions.GetCurrentDatabase());
      foreach (DataRow row in (InternalDataCollectionBase) new VistaDBConnection(VistaDBContext.DDAChannel.CurrentDatabase).GetSchema("PROCEDUREPARAMETERS").Rows)
        parameterSchemaEntryList.Add(new SystemFunctions.ParameterSchemaEntry(databaseName, row));
      return (IEnumerable) parameterSchemaEntryList;
    }

    public static void FillParameterSchema(object source, VistaDBString specific_catalog, VistaDBString specific_schema, VistaDBString specific_name, VistaDBInt32 ordinal_position, VistaDBString parameter_mode, VistaDBString is_result, VistaDBString as_locator, VistaDBString parameter_name, VistaDBString data_type, VistaDBInt32 character_maximum_length, VistaDBInt32 character_octet_length, VistaDBString collation_catalog, VistaDBString collation_schema, VistaDBString collation_name, VistaDBString character_set_catalog, VistaDBString character_set_schema, VistaDBString character_set_name, VistaDBByte numeric_precision, VistaDBInt16 numeric_precision_radix, VistaDBInt16 numeric_scale, VistaDBInt16 datetime_precision, VistaDBString interval_type, VistaDBInt16 interval_precision, VistaDBString user_defined_type_catalog, VistaDBString user_defined_type_schema, VistaDBString user_defined_type_name, VistaDBString scope_catalog, VistaDBString scope_schema, VistaDBString scope_name)
    {
      SystemFunctions.ParameterSchemaEntry parameterSchemaEntry = source as SystemFunctions.ParameterSchemaEntry;
      specific_catalog.Value = (object) parameterSchemaEntry.SpecificCatalog;
      specific_schema.Value = (object) parameterSchemaEntry.SpecificSchema;
      specific_name.Value = (object) parameterSchemaEntry.SpecificName;
      if (parameterSchemaEntry.ParameterMode == 1)
        parameter_mode.Value = (object) "OUT";
      else
        parameter_mode.Value = (object) "IN";
      ordinal_position.Value = (object) ((int) parameterSchemaEntry.OrdinalPosition + 1);
      is_result.Value = parameterSchemaEntry.IsResult ? (object) "YES" : (object) "NO";
      as_locator.Value = parameterSchemaEntry.AsLocator ? (object) "YES" : (object) "NO";
      parameter_name.Value = (object) parameterSchemaEntry.ParameterName;
      data_type.Value = (object) parameterSchemaEntry.DataType.ToLowerInvariant();
      character_maximum_length.Value = (object) parameterSchemaEntry.CharacterMaxLength;
      character_octet_length.Value = (object) parameterSchemaEntry.CharacterOctetLength;
      collation_catalog.Value = (object) parameterSchemaEntry.CollationCatalog;
      collation_schema.Value = (object) parameterSchemaEntry.CollationSchema;
      collation_name.Value = (object) parameterSchemaEntry.CollationName;
      character_set_catalog.Value = (object) parameterSchemaEntry.CharacterSetCatalog;
      character_set_schema.Value = (object) parameterSchemaEntry.CharacterSetSchema;
      character_set_name.Value = (object) parameterSchemaEntry.CharacterSetName;
      numeric_precision.Value = (object) parameterSchemaEntry.NumericPrecision;
      numeric_precision_radix.Value = (object) parameterSchemaEntry.NumericPrecisionRadix;
      numeric_scale.Value = (object) parameterSchemaEntry.NumericScale;
      datetime_precision.Value = (object) parameterSchemaEntry.DateTimePrecision;
      interval_type.Value = (object) parameterSchemaEntry.IntervalType;
      interval_precision.Value = (object) parameterSchemaEntry.IntervalPrecision;
      user_defined_type_catalog.Value = (object) parameterSchemaEntry.UserDefinedTypeCatalog;
      user_defined_type_schema.Value = (object) parameterSchemaEntry.UserDefinedTypeSchema;
      user_defined_type_name.Value = (object) parameterSchemaEntry.UserDefinedTypeName;
      scope_catalog.Value = (object) parameterSchemaEntry.ScopeCatalog;
      scope_schema.Value = (object) parameterSchemaEntry.ScopeSchema;
      scope_name.Value = (object) parameterSchemaEntry.ScopeName;
    }

    [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillRoutineColumnSchema")]
    public static IEnumerable RoutineColumnSchema()
    {
      return (IEnumerable) new List<object>();
    }

    public static void FillRoutineColumnSchema(object source, VistaDBString table_catalog, VistaDBString table_schema, VistaDBString table_name, VistaDBString column_name, VistaDBInt32 ordinal_position, VistaDBString column_default, VistaDBString is_nullable, VistaDBString data_type, VistaDBInt32 character_maximum_length, VistaDBInt32 character_octet_length, VistaDBByte numeric_precision, VistaDBInt16 numeric_precision_radix, VistaDBInt16 numeric_scale, VistaDBInt16 datetime_precision, VistaDBString character_set_catalog, VistaDBString character_set_schema, VistaDBString character_set_name, VistaDBString collation_catalog, VistaDBString collation_schema, VistaDBString collation_name, VistaDBString domain_catalog, VistaDBString domain_schema, VistaDBString domain_name)
    {
    }

    [VistaDBClrProcedure]
    public static VistaDBInt32 ObjectId(VistaDBString object_name)
    {
      return SystemFunctions.ObjectId2(object_name, new VistaDBString());
    }

    public static VistaDBInt32 ObjectId2(VistaDBString object_name, VistaDBString object_type)
    {
      string[] strArray = ((string) object_name.Value).Split(new char[3]{ '.', '[', ']' }, StringSplitOptions.RemoveEmptyEntries);
      string str = strArray[strArray.Length - 1];
      int hashCode = str.GetHashCode();
      if (!SystemFunctions._objIds.ContainsKey(hashCode))
        SystemFunctions._objIds.Add(hashCode, str);
      return new VistaDBInt32(hashCode);
    }

    public static VistaDBString DatabaseName()
    {
      Database currentDatabase = SystemFunctions.GetCurrentDatabase();
      if (currentDatabase != null)
        return new VistaDBString(SystemFunctions.GetDatabaseName(currentDatabase));
      using (VistaDBConnection currentConnection = SystemFunctions.GetCurrentConnection())
      {
        if (currentConnection != null)
          return new VistaDBString(SystemFunctions.GetDatabaseName(currentConnection.Database));
      }
      return new VistaDBString();
    }

    [VistaDBClrProcedure]
    public static VistaDBInt32 ColumnProperty(VistaDBInt32 id, VistaDBString column, VistaDBString property)
    {
      if (!SystemFunctions._objIds.ContainsKey((int) id.Value))
        return new VistaDBInt32();
      string objId = SystemFunctions._objIds[(int) id.Value];
      string index = (string) column.Value;
      string str = (string) property.Value;
      Database currentDatabase = SystemFunctions.GetCurrentDatabase();
      IVistaDBTableSchema vistaDbTableSchema = (IVistaDBTableSchema) null;
      if (currentDatabase != null)
      {
        vistaDbTableSchema = (IVistaDBTableSchema) currentDatabase.GetTableSchema(objId, true);
      }
      else
      {
        using (VistaDBConnection currentConnection = SystemFunctions.GetCurrentConnection())
          vistaDbTableSchema = currentConnection.GetTableSchema(objId);
      }
      if (vistaDbTableSchema == null)
        return new VistaDBInt32();
      int val = 0;
      using (vistaDbTableSchema)
      {
        switch (str.ToLower())
        {
          case "isidentity":
            val = vistaDbTableSchema.Identities[index] != null ? 1 : 0;
            break;
          default:
            return new VistaDBInt32();
        }
      }
      return new VistaDBInt32(val);
    }

    [VistaDBClrProcedure]
    public static VistaDBString ServerProperty(VistaDBString propertyName)
    {
      switch (((string) propertyName.Value).ToLowerInvariant())
      {
        case "productversion":
          return new VistaDBString("4.34.3");
        case "servername":
          return new VistaDBString("localhost");
        case "instancename":
          return new VistaDBString("VistaDB");
        default:
          return new VistaDBString();
      }
    }

    [VistaDBClrProcedure]
    public static VistaDBInt64 UserId()
    {
      return new VistaDBInt64(0L);
    }

    [VistaDBClrProcedure]
    public static VistaDBString UserName(VistaDBInt64 id)
    {
      if ((long) id.Value == 0L)
        return new VistaDBString("sa");
      return SystemFunctions.SchemaName();
    }

    [VistaDBClrProcedure]
    public static VistaDBString SchemaName()
    {
      return new VistaDBString("dbo");
    }

    public static void RegisterIntoHosting(ClrHosting hosting, IList<string> systemProcedures)
    {
      Assembly assembly = SystemFunctions.GetAssembly();
      if (assembly == null)
        return;
      try
      {
        ClrHosting.ClrProcedure method1 = SystemFunctions.GetMethod("TableSchema", assembly);
        hosting.AddProcedure("VistaDBTableSchema", method1);
        ClrHosting.ClrProcedure method2 = SystemFunctions.GetMethod("EF_Tables_Query", assembly);
        hosting.AddProcedure("VistaDBEFTables", method2);
        ClrHosting.ClrProcedure method3 = SystemFunctions.GetMethod("EF_TableColumns_Query", assembly);
        hosting.AddProcedure("VistaDBEFTableColumns", method3);
        ClrHosting.ClrProcedure method4 = SystemFunctions.GetMethod("EF_Views_Query", assembly);
        hosting.AddProcedure("VistaDBEFViews", method4);
        ClrHosting.ClrProcedure method5 = SystemFunctions.GetMethod("EF_ViewColumns_Query", assembly);
        hosting.AddProcedure("VistaDBEFViewColumns", method5);
        ClrHosting.ClrProcedure method6 = SystemFunctions.GetMethod("EF_Constraints_Query", assembly);
        hosting.AddProcedure("VistaDBEFConstraints", method6);
        ClrHosting.ClrProcedure method7 = SystemFunctions.GetMethod("EF_ConstraintColumns_Query", assembly);
        hosting.AddProcedure("VistaDBEFConstraintColumns", method7);
        ClrHosting.ClrProcedure method8 = SystemFunctions.GetMethod("EF_ForeignKeyConstraints_Query", assembly);
        hosting.AddProcedure("VistaDBEFForeignKeyConstraints", method8);
        ClrHosting.ClrProcedure method9 = SystemFunctions.GetMethod("EF_ForeignKeys_Query", assembly);
        hosting.AddProcedure("VistaDBEFForeignKeys", method9);
        ClrHosting.ClrProcedure method10 = SystemFunctions.GetMethod("ColumnSchema", assembly);
        hosting.AddProcedure("VistaDBColumnSchema", method10);
        ClrHosting.ClrProcedure method11 = SystemFunctions.GetMethod("ViewSchema", assembly);
        hosting.AddProcedure("VistaDBViewSchema", method11);
        ClrHosting.ClrProcedure method12 = SystemFunctions.GetMethod("TableConstraintSchema", assembly);
        hosting.AddProcedure("VistaDBTableConstraintSchema", method12);
        ClrHosting.ClrProcedure method13 = SystemFunctions.GetMethod("CheckConstraintSchema", assembly);
        hosting.AddProcedure("VistaDBCheckConstraintSchema", method13);
        ClrHosting.ClrProcedure method14 = SystemFunctions.GetMethod("ReferentialConstraintSchema", assembly);
        hosting.AddProcedure("VistaDBReferentialConstraintSchema", method14);
        ClrHosting.ClrProcedure method15 = SystemFunctions.GetMethod("ForeignKeySchema", assembly);
        hosting.AddProcedure("VistaDBForeignKeySchema", method15);
        ClrHosting.ClrProcedure method16 = SystemFunctions.GetMethod("ForeignKeyColumnSchema", assembly);
        hosting.AddProcedure("VistaDBForeignKeyColumnSchema", method16);
        ClrHosting.ClrProcedure method17 = SystemFunctions.GetMethod("IndexSchema", assembly);
        hosting.AddProcedure("VistaDBIndexSchema", method17);
        ClrHosting.ClrProcedure method18 = SystemFunctions.GetMethod("IndexColumnSchema", assembly);
        hosting.AddProcedure("VistaDBIndexColumnSchema", method18);
        ClrHosting.ClrProcedure method19 = SystemFunctions.GetMethod("KeyColumnUsageSchema", assembly);
        hosting.AddProcedure("VistaDBKeyColumnUsageSchema", method19);
        ClrHosting.ClrProcedure method20 = SystemFunctions.GetMethod("RoutineSchema", assembly);
        hosting.AddProcedure("VistaDBRoutineSchema", method20);
        ClrHosting.ClrProcedure method21 = SystemFunctions.GetMethod("ParameterSchema", assembly);
        hosting.AddProcedure("VistaDBParameterSchema", method21);
        ClrHosting.ClrProcedure method22 = SystemFunctions.GetMethod("ObjectId", assembly);
        hosting.AddProcedure("Object_Id", method22);
        systemProcedures.Add("Object_Id");
        ClrHosting.ClrProcedure method23 = SystemFunctions.GetMethod("ColumnProperty", assembly);
        hosting.AddProcedure("ColumnProperty", method23);
        systemProcedures.Add("ColumnProperty");
        ClrHosting.ClrProcedure method24 = SystemFunctions.GetMethod("ServerProperty", assembly);
        hosting.AddProcedure("ServerProperty", method24);
        systemProcedures.Add("ServerProperty");
        ClrHosting.ClrProcedure method25 = SystemFunctions.GetMethod("UserId", assembly);
        hosting.AddProcedure("SUser_SID", method25);
        systemProcedures.Add("SUser_SID");
        ClrHosting.ClrProcedure method26 = SystemFunctions.GetMethod("UserName", assembly);
        hosting.AddProcedure("SUser_SNAME", method26);
        systemProcedures.Add("SUser_SNAME");
        ClrHosting.ClrProcedure method27 = SystemFunctions.GetMethod("SchemaName", assembly);
        hosting.AddProcedure("Schema_Name", method27);
        systemProcedures.Add("Schema_Name");
        hosting.AddProcedure("User_Name", method27);
        systemProcedures.Add("User_Name");
        ClrHosting.ClrProcedure method28 = SystemFunctions.GetMethod("DatabaseName", assembly);
        hosting.AddProcedure("DB_Name", method28);
        systemProcedures.Add("DB_Name");
      }
      catch (Exception ex)
      {
      }
    }

    private class TableSchemaEntry
    {
      private string _db;
      private string _name;
      private string _owner;
      private string _type;

      public TableSchemaEntry(string db_name, IVistaDBTableSchema schema)
      {
        this._db = db_name;
        this._name = schema.Name;
        this._owner = "dbo";
        this._type = "BASE TABLE";
      }

      public TableSchemaEntry(string db_name, Database.ViewList.View schema)
      {
        this._db = db_name;
        this._name = schema.Name;
        this._owner = "dbo";
        this._type = "VIEW";
      }

      public string Catalog
      {
        get
        {
          return this._db;
        }
      }

      public string Name
      {
        get
        {
          return this._name;
        }
      }

      public string Owner
      {
        get
        {
          return this._owner;
        }
      }

      public string TableType
      {
        get
        {
          return this._type;
        }
      }
    }

    private class ColumnSchemaEntry
    {
      private string _db;
      private string _owner;
      private string _table;
      private string _name;
      private int _index;
      private string _default;
      private bool _nullable;
      private VistaDBType _type;
      private int _maxLength;
      private int _precision;
      private short _scale;
      private int _octetLength;
      private short _radix;
      private short _dateTime;
      private bool _identity;
      private bool _isStoreGenerated;

      public ColumnSchemaEntry(string db_name, IVistaDBTableSchema table, IVistaDBColumnAttributes schema, IVistaDBDefaultValueInformation defaults, IVistaDBIdentityInformation identity)
      {
        this._db = db_name;
        this._owner = "dbo";
        this._table = table.Name;
        this._name = schema.Name;
        this._isStoreGenerated = schema.Type == VistaDBType.Timestamp;
        this._index = schema.RowIndex;
        if (defaults != null)
          this._default = defaults.Expression;
        this._nullable = schema.AllowNull;
        this._type = schema.Type;
        this._maxLength = schema.MaxLength;
        this._identity = identity != null;
        SystemFunctions.GetTypeInfo(this._table, this._name, this._type, ref this._maxLength, ref this._octetLength, ref this._precision, ref this._scale, ref this._radix, ref this._dateTime);
      }

      public ColumnSchemaEntry(string catalog, DataRow column)
      {
        this._db = catalog;
        this._owner = "dbo";
        this._table = column["VIEW_NAME"] as string;
        this._name = column["COLUMN_NAME"] as string;
        this._index = (int) column["ORDINAL_POSITION"];
        if ((bool) column["COLUMN_HASDEFAULT"])
          this._default = column["COLUMN_DEFAULT"] as string;
        this._nullable = (bool) column["IS_NULLABLE"];
        this._type = (VistaDBType) Enum.Parse(typeof (VistaDBType), column["DATA_TYPE"] as string);
        this._isStoreGenerated = this._type == VistaDBType.Timestamp;
        this._maxLength = (int) column["CHARACTER_MAXIMUM_LENGTH"];
        SystemFunctions.GetTypeInfo(this._table, this._name, this._type, ref this._maxLength, ref this._octetLength, ref this._precision, ref this._scale, ref this._radix, ref this._dateTime);
      }

      public string Catalog
      {
        get
        {
          return this._db;
        }
      }

      public string Owner
      {
        get
        {
          return this._owner;
        }
      }

      public string Table
      {
        get
        {
          return this._table;
        }
      }

      public string Name
      {
        get
        {
          return this._name;
        }
      }

      public int Index
      {
        get
        {
          return this._index;
        }
      }

      public string Default
      {
        get
        {
          return this._default;
        }
      }

      public bool Nullable
      {
        get
        {
          return this._nullable;
        }
      }

      public VistaDBType DataType
      {
        get
        {
          return this._type;
        }
      }

      public int MaxLength
      {
        get
        {
          return this._maxLength;
        }
      }

      public int Precision
      {
        get
        {
          return this._precision;
        }
      }

      public short Scale
      {
        get
        {
          return this._scale;
        }
      }

      public int OctetLength
      {
        get
        {
          return this._octetLength;
        }
      }

      public short Radix
      {
        get
        {
          return this._radix;
        }
      }

      public short DateTimeSub
      {
        get
        {
          return this._dateTime;
        }
      }

      public bool IsIdentity
      {
        get
        {
          return this._identity;
        }
      }

      public bool IsStoreGenerated
      {
        get
        {
          return this._isStoreGenerated;
        }
      }
    }

    private class ViewSchemaEntry
    {
      private string _db;
      private string _name;
      private string _def;
      private string _owner;

      public ViewSchemaEntry(string catalog, Database.ViewList.View schema)
      {
        this._db = catalog;
        this._name = schema.Name;
        this._def = schema.Expression;
        this._owner = "dbo";
      }

      public string Catalog
      {
        get
        {
          return this._db;
        }
      }

      public string Name
      {
        get
        {
          return this._name;
        }
      }

      public string Expression
      {
        get
        {
          return this._def;
        }
      }

      public string Owner
      {
        get
        {
          return this._owner;
        }
      }
    }

    private class ViewColumnDataRow
    {
      public ViewColumnDataRow(string catalog, DataRow row)
      {
        this.ViewCatalog = row["VIEW_CATALOG"] as string ?? catalog;
        this.ViewSchema = row["VIEW_SCHEMA"] as string ?? "dbo";
        this.ViewName = row["VIEW_NAME"] as string;
        this.TableCatalog = row["TABLE_CATALOG"] as string ?? catalog;
        this.TableSchema = row["TABLE_SCHEMA"] as string ?? "dbo";
        this.TableName = row["TABLE_NAME"] as string;
        this.Name = row["COLUMN_NAME"] as string;
      }

      public string ViewCatalog { get; private set; }

      public string ViewSchema { get; private set; }

      public string ViewName { get; private set; }

      public string TableCatalog { get; private set; }

      public string TableSchema { get; private set; }

      public string TableName { get; private set; }

      public string Name { get; private set; }
    }

    private class RelationshipSchemaEntry
    {
      private string _db;
      private string _name;
      private string _table;
      private string _owner;
      private string _expression;
      private string _type;
      private string _update;
      private string _delete;
      private int _ordinal;

      public RelationshipSchemaEntry(string db_name, string tableName, IVistaDBIndexInformation index)
      {
        this._owner = "dbo";
        this._db = db_name;
        this._name = index.Name;
        this._table = tableName;
        if (index.Primary)
          this._type = "PRIMARY KEY";
        else if (index.Unique)
        {
          this._type = "UNIQUE";
        }
        else
        {
          if (!index.FKConstraint)
            throw new Exception("Only call this constructor on primary or unique indexes!");
          this._type = "FOREIGN KEY";
        }
        this._expression = index.KeyExpression;
      }

      public RelationshipSchemaEntry(string db_name, string tableName, IVistaDBConstraintInformation constraint)
      {
        this._owner = "dbo";
        this._db = db_name;
        this._name = constraint.Name;
        this._table = tableName;
        this._expression = constraint.Expression;
        this._type = "CHECK";
      }

      public RelationshipSchemaEntry(string db_name, IVistaDBRelationshipInformation relation, IVistaDBIndexInformation foreign_index)
      {
        this._owner = "dbo";
        this._db = db_name;
        this._name = relation.Name;
        this._table = relation.ForeignTable;
        this._expression = foreign_index.Name;
        this._type = "FOREIGN KEY";
        switch (relation.UpdateIntegrity)
        {
          case VistaDBReferentialIntegrity.Cascade:
            this._update = "CASCADE";
            break;
          case VistaDBReferentialIntegrity.SetNull:
            this._update = "SET NULL";
            break;
          case VistaDBReferentialIntegrity.SetDefault:
            this._update = "SET DEFAULT";
            break;
          default:
            this._update = "NO ACTION";
            break;
        }
        switch (relation.DeleteIntegrity)
        {
          case VistaDBReferentialIntegrity.Cascade:
            this._delete = "CASCADE";
            break;
          case VistaDBReferentialIntegrity.SetNull:
            this._delete = "SET NULL";
            break;
          case VistaDBReferentialIntegrity.SetDefault:
            this._delete = "SET DEFAULT";
            break;
          default:
            this._delete = "NO ACTION";
            break;
        }
      }

      public RelationshipSchemaEntry(string db_name, IVistaDBTableSchema table, IVistaDBIndexInformation index, IVistaDBTableSchema primaryTable, IVistaDBKeyColumn key)
      {
        this._db = db_name;
        this._table = table.Name;
        this._owner = "dbo";
        this._name = index.Name;
        this._expression = primaryTable[key.RowIndex].Name;
        this._ordinal = key.RowIndex + 1;
      }

      public string Catalog
      {
        get
        {
          return this._db;
        }
      }

      public string Name
      {
        get
        {
          return this._name;
        }
      }

      public string Owner
      {
        get
        {
          return this._owner;
        }
      }

      public string Table
      {
        get
        {
          return this._table;
        }
      }

      public string Expression
      {
        get
        {
          return this._expression;
        }
      }

      public string RelationType
      {
        get
        {
          return this._type;
        }
      }

      public string UpdateRule
      {
        get
        {
          return this._update;
        }
      }

      public string DeleteRule
      {
        get
        {
          return this._delete;
        }
      }

      public int ColumnOrdinal
      {
        get
        {
          return this._ordinal;
        }
      }
    }

    private class ForeignKeySchemaEntry
    {
      private string _db;
      private string _name;
      private string _table;
      private string _primary;
      private string _owner;
      private int _update;
      private int _delete;

      public ForeignKeySchemaEntry(string db_name, IVistaDBTableSchema schema, IVistaDBIndexInformation index, IVistaDBRelationshipInformation relation)
      {
        this._db = db_name;
        this._name = index.Name;
        this._table = schema.Name;
        this._primary = relation.PrimaryTable;
        this._owner = "dbo";
        this._delete = (int) relation.DeleteIntegrity;
        this._update = (int) relation.UpdateIntegrity;
      }

      public string Catalog
      {
        get
        {
          return this._db;
        }
      }

      public string Name
      {
        get
        {
          return this._name;
        }
      }

      public string Owner
      {
        get
        {
          return this._owner;
        }
      }

      public string Table
      {
        get
        {
          return this._table;
        }
      }

      public string TargetTable
      {
        get
        {
          return this._primary;
        }
      }

      public int Update
      {
        get
        {
          return this._update;
        }
      }

      public int Delete
      {
        get
        {
          return this._delete;
        }
      }
    }

    private class ForeignKeyColumnSchemaEntry
    {
      private string _db;
      private string _name;
      private string _table;
      private string _key;
      private string _column;
      private string _owner;
      private int _ordinal;

      public ForeignKeyColumnSchemaEntry(string db_name, IVistaDBTableSchema schema, IVistaDBIndexInformation index, IVistaDBRelationshipInformation relation, IVistaDBTableSchema targetSchema, IVistaDBKeyColumn column, IVistaDBIndexInformation targetKey)
      {
        this._db = db_name;
        this._name = index.Name;
        this._table = schema.Name;
        this._key = schema[column.RowIndex].Name;
        this._column = targetSchema[targetKey.KeyStructure[0].RowIndex].Name;
        this._ordinal = targetKey.KeyStructure[0].RowIndex;
        this._owner = "dbo";
      }

      public string Catalog
      {
        get
        {
          return this._db;
        }
      }

      public string Name
      {
        get
        {
          return this._name;
        }
      }

      public string Owner
      {
        get
        {
          return this._owner;
        }
      }

      public string Table
      {
        get
        {
          return this._table;
        }
      }

      public string Key
      {
        get
        {
          return this._key;
        }
      }

      public string Column
      {
        get
        {
          return this._column;
        }
      }

      public int Ordinal
      {
        get
        {
          return this._ordinal;
        }
      }
    }

    private class IndexSchemaEntry
    {
      private string _db;
      private string _name;
      private string _table;
      private string _owner;
      private bool _primary;
      private bool _unique;

      public IndexSchemaEntry(string db_name, IVistaDBTableSchema schema, IVistaDBIndexInformation index)
      {
        this._db = db_name;
        this._name = index.Name;
        this._table = schema.Name;
        this._owner = "dbo";
        this._primary = index.Primary;
        this._unique = index.Unique;
      }

      public string Catalog
      {
        get
        {
          return this._db;
        }
      }

      public string Name
      {
        get
        {
          return this._name;
        }
      }

      public string Owner
      {
        get
        {
          return this._owner;
        }
      }

      public string Table
      {
        get
        {
          return this._table;
        }
      }

      public bool IsPrimary
      {
        get
        {
          return this._primary;
        }
      }

      public bool IsUnique
      {
        get
        {
          return this._unique;
        }
      }
    }

    private class IndexColumnSchemaEntry
    {
      private string _db;
      private string _index;
      private string _table;
      private string _owner;
      private string _name;
      private int _ordinal;

      public IndexColumnSchemaEntry(string db_name, IVistaDBTableSchema schema, IVistaDBIndexInformation index, IVistaDBKeyColumn column)
      {
        this._db = db_name;
        this._index = index.Name;
        this._table = schema.Name;
        this._owner = "dbo";
        this._name = schema[column.RowIndex].Name;
        this._ordinal = column.RowIndex;
      }

      public string Catalog
      {
        get
        {
          return this._db;
        }
      }

      public string Name
      {
        get
        {
          return this._name;
        }
      }

      public string Owner
      {
        get
        {
          return this._owner;
        }
      }

      public string Table
      {
        get
        {
          return this._table;
        }
      }

      public string Index
      {
        get
        {
          return this._index;
        }
      }

      public int Ordinal
      {
        get
        {
          return this._ordinal;
        }
      }
    }

    private class RoutineSchemaEntry
    {
      private string _db;
      private string _name;
      private string _owner;
      private bool _func;
      private bool _table;
      private VistaDBType _type;
      private int _maxLength;
      private int _precision;
      private short _scale;
      private int _octetLength;
      private short _radix;
      private short _dateTime;
      private string _body;
      private int _results;
      private DateTime _created;
      private DateTime _altered;

      public RoutineSchemaEntry(string db, IStoredProcedureInformation sp, Statement statement, List<SQLParser.VariableDeclaration> variables)
      {
        this._db = db;
        this._owner = "dbo";
        this._func = false;
        this._table = false;
        this._name = sp.Name;
        this._body = sp.Statement;
        SystemFunctions.GetTypeInfo(this._name, "Return", this._type, ref this._maxLength, ref this._octetLength, ref this._precision, ref this._scale, ref this._radix, ref this._dateTime);
      }

      public string Catalog
      {
        get
        {
          return this._db;
        }
      }

      public string Owner
      {
        get
        {
          return this._owner;
        }
      }

      public string Name
      {
        get
        {
          return this._name;
        }
      }

      public bool IsFunction
      {
        get
        {
          return this._func;
        }
      }

      public bool IsTable
      {
        get
        {
          return this._table;
        }
      }

      public VistaDBType DataType
      {
        get
        {
          return this._type;
        }
      }

      public int MaxLength
      {
        get
        {
          return this._maxLength;
        }
      }

      public int Precision
      {
        get
        {
          return this._precision;
        }
      }

      public short Scale
      {
        get
        {
          return this._scale;
        }
      }

      public int OctetLength
      {
        get
        {
          return this._octetLength;
        }
      }

      public short Radix
      {
        get
        {
          return this._radix;
        }
      }

      public short DateTimeSub
      {
        get
        {
          return this._dateTime;
        }
      }

      public string Body
      {
        get
        {
          return this._body;
        }
      }

      public int MaxResults
      {
        get
        {
          return this._results;
        }
      }

      public DateTime Created
      {
        get
        {
          return this._created;
        }
      }

      public DateTime LastAltered
      {
        get
        {
          return this._altered;
        }
      }
    }

    private class ParameterSchemaEntry
    {
      public ParameterSchemaEntry(string catalog, DataRow row)
      {
        this.SpecificCatalog = row["SPECIFIC_CATALOG"] as string ?? catalog;
        this.SpecificSchema = row["SPECIFIC_SCHEMA"] as string ?? "dbo";
        this.SpecificName = row["PROCEDURE_NAME"] as string;
        this.OrdinalPosition = Convert.ToInt16(row["ORDINAL_POSITION"]);
        this.ParameterMode = Convert.ToInt32(row["PARAMETER_DIRECTION"]);
        this.IsResult = false;
        this.AsLocator = false;
        this.ParameterName = row["PARAMETER_NAME"] as string;
        this.DataType = row["PARAMETER_DATA_TYPE"] as string;
        VistaDBType type = (VistaDBType) Enum.Parse(typeof (VistaDBType), this.DataType);
        this.CharacterMaxLength = new int?();
        this.CharacterOctetLength = new int?();
        this.CharacterSetCatalog = (string) null;
        this.CharacterSetSchema = (string) null;
        this.CharacterSetName = (string) null;
        this.CollationCatalog = (string) null;
        this.CollationSchema = (string) null;
        this.CollationName = (string) null;
        this.NumericPrecision = new byte?();
        this.NumericPrecisionRadix = new short?();
        this.NumericScale = new short?();
        this.DateTimePrecision = new short?();
        this.IntervalType = (string) null;
        this.IntervalPrecision = new short?();
        this.UserDefinedTypeCatalog = (string) null;
        this.UserDefinedTypeSchema = (string) null;
        this.UserDefinedTypeName = (string) null;
        this.ScopeCatalog = (string) null;
        this.ScopeSchema = (string) null;
        this.SpecificCatalog = (string) null;
        int maxLength = 0;
        int octetLength = 0;
        int precision = 0;
        short scale = 0;
        short radix = 0;
        short dateTime = 0;
        switch (type)
        {
          case VistaDBType.Char:
          case VistaDBType.VarChar:
            maxLength = 8000;
            break;
          case VistaDBType.NChar:
          case VistaDBType.NVarChar:
            maxLength = 4000;
            break;
          case VistaDBType.Text:
            maxLength = int.MaxValue;
            break;
          case VistaDBType.NText:
            maxLength = 1073741823;
            break;
          case VistaDBType.Image:
            maxLength = int.MaxValue;
            break;
        }
        SystemFunctions.GetTypeInfo(this.SpecificName, this.ParameterName, type, ref maxLength, ref octetLength, ref precision, ref scale, ref radix, ref dateTime);
        if (maxLength != 0)
          this.CharacterMaxLength = new int?(maxLength);
        if (octetLength != -1)
          this.CharacterOctetLength = new int?(octetLength);
        if (precision != 0 && precision < (int) byte.MaxValue)
          this.NumericPrecision = new byte?((byte) precision);
        if (scale != (short) -1)
          this.NumericScale = new short?(scale);
        if (radix != (short) 0)
          this.NumericPrecisionRadix = new short?(radix);
        if (dateTime == (short) 0)
          return;
        this.DateTimePrecision = new short?(dateTime);
      }

      public string SpecificCatalog { get; private set; }

      public string SpecificSchema { get; private set; }

      public string SpecificName { get; private set; }

      public short OrdinalPosition { get; private set; }

      public int ParameterMode { get; private set; }

      public bool IsResult { get; private set; }

      public bool AsLocator { get; private set; }

      public string ParameterName { get; private set; }

      public string DataType { get; private set; }

      public int? CharacterMaxLength { get; private set; }

      public int? CharacterOctetLength { get; private set; }

      public string CharacterSetCatalog { get; private set; }

      public string CharacterSetSchema { get; private set; }

      public string CharacterSetName { get; private set; }

      public string CollationCatalog { get; private set; }

      public string CollationSchema { get; private set; }

      public string CollationName { get; private set; }

      public byte? NumericPrecision { get; private set; }

      public short? NumericPrecisionRadix { get; private set; }

      public short? NumericScale { get; private set; }

      public short? DateTimePrecision { get; private set; }

      public string IntervalType { get; private set; }

      public short? IntervalPrecision { get; private set; }

      public string UserDefinedTypeCatalog { get; private set; }

      public string UserDefinedTypeSchema { get; private set; }

      public string UserDefinedTypeName { get; private set; }

      public string ScopeCatalog { get; private set; }

      public string ScopeSchema { get; private set; }

      public string ScopeName { get; private set; }
    }
  }
}
