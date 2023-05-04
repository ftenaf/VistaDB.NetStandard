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
                return (Database)((Table)VistaDBContext.DDAChannel.CurrentDatabase).Rowset;
            if (VistaDBContext.SQLChannel.IsAvailable)
                return (Database)VistaDBContext.SQLChannel.CurrentConnection;
            return null;
        }

        private static VistaDBConnection GetCurrentConnection()
        {
            return new VistaDBConnection("Context connection= true");
        }

        private static string GetDatabaseName(Database database)
        {
            return GetDatabaseName(database.Name);
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
            return typeof(SystemFunctions).Assembly;
        }

        private static ClrHosting.ClrProcedure GetMethod(string name, Assembly assembly)
        {
            Type type = typeof(SystemFunctions);
            MethodInfo method = type.GetMethod(name);
            MethodInfo fillRowProcedure = null;
            if (method == null)
                return null;
            foreach (VistaDBClrProcedureAttribute customAttribute in method.GetCustomAttributes(typeof(VistaDBClrProcedureAttribute), false))
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
            scale = -1;
            radix = 0;
            dateTime = 0;
            switch (type)
            {
                case VistaDBType.Char:
                    if (maxLength < 1 || maxLength > 8000)
                        throw new Exception(string.Format("Broken database detected! ([{0}].[{1}] {2}) has an invalid length of {3}, must be between {4} and {5}", table, column, type.ToString(), maxLength, 1, 8000));
                    precision = maxLength;
                    octetLength = maxLength;
                    break;
                case VistaDBType.NChar:
                    if (maxLength < 1 || maxLength > 8000)
                        throw new Exception(string.Format("Broken database detected! ([{0}].[{1}] {2}) has an invalid length of {3}, must be between {4} and {5}", table, column, type.ToString(), maxLength, 1, 8000));
                    precision = maxLength / 2;
                    octetLength = maxLength;
                    break;
                case VistaDBType.VarChar:
                    if (maxLength < 1 || maxLength > 8000)
                        throw new Exception(string.Format("Broken database detected! ([{0}].[{1}] {2}) has an invalid length of {3}, must be between {4} and {5}", table, column, type.ToString(), maxLength, 1, 8000));
                    precision = maxLength;
                    octetLength = maxLength;
                    break;
                case VistaDBType.NVarChar:
                    if (maxLength < 1 || maxLength > 8000)
                        throw new Exception(string.Format("Broken database detected! ([{0}].[{1}] {2}) has an invalid length of {3}, must be between {4} and {5}", table, column, type.ToString(), maxLength, 1, 8000));
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
                    scale = 0;
                    radix = 10;
                    break;
                case VistaDBType.SmallInt:
                    precision = 5;
                    scale = 0;
                    radix = 10;
                    break;
                case VistaDBType.Int:
                    precision = 10;
                    scale = 0;
                    radix = 10;
                    break;
                case VistaDBType.BigInt:
                    precision = 19;
                    scale = 0;
                    radix = 10;
                    break;
                case VistaDBType.Real:
                    precision = 7;
                    radix = 10;
                    break;
                case VistaDBType.Float:
                    precision = 15;
                    radix = 10;
                    break;
                case VistaDBType.Decimal:
                    precision = 18;
                    scale = 0;
                    radix = 10;
                    break;
                case VistaDBType.Money:
                    precision = 19;
                    scale = 4;
                    radix = 10;
                    break;
                case VistaDBType.SmallMoney:
                    precision = 10;
                    scale = 4;
                    radix = 10;
                    break;
                case VistaDBType.Bit:
                    precision = 1;
                    break;
                case VistaDBType.DateTime:
                    precision = 23;
                    scale = 3;
                    dateTime = 3;
                    break;
                case VistaDBType.Image:
                    precision = int.MaxValue;
                    octetLength = maxLength;
                    break;
                case VistaDBType.VarBinary:
                    if (maxLength < 1 || maxLength > 8000)
                        throw new Exception(string.Format("Broken database detected! ([{0}].[{1}] {2}) has an invalid length of {3}, must be between {4} and {5}", table, column, type.ToString(), maxLength, 1, 8000));
                    precision = maxLength;
                    octetLength = maxLength;
                    break;
                case VistaDBType.UniqueIdentifier:
                    precision = 36;
                    break;
                case VistaDBType.SmallDateTime:
                    precision = 16;
                    scale = 0;
                    dateTime = -1;
                    break;
                case VistaDBType.Timestamp:
                    precision = 8;
                    octetLength = maxLength;
                    break;
            }
        }

        private static Statement ParseProcedure(Database db, string body, out List<SQLParser.VariableDeclaration> variables)
        {
            return ((LocalSQLConnection)VistaDBContext.SQLChannel.CurrentConnection).CreateStoredProcedureStatement(null, body, out variables);
        }

        [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillTableSchema")]
        public static IEnumerable TableSchema()
        {
            List<TableSchemaEntry> tableSchemaEntryList = new List<TableSchemaEntry>();
            Database currentDatabase = GetCurrentDatabase();
            string databaseName = GetDatabaseName(currentDatabase);
            foreach (string key in (IEnumerable<string>)currentDatabase.GetTableIdMap().Keys)
            {
                IVistaDBTableSchema tableSchema = currentDatabase.GetTableSchema(key, false);
                tableSchemaEntryList.Add(new TableSchemaEntry(databaseName, tableSchema));
            }
            foreach (Database.ViewList.View schema in (IEnumerable)currentDatabase.LoadViews().Values)
                tableSchemaEntryList.Add(new TableSchemaEntry(databaseName, schema));
            return tableSchemaEntryList;
        }

        public static void FillTableSchema(object source, VistaDBString table_catalog, VistaDBString table_schema, VistaDBString table_name, VistaDBString table_type)
        {
            TableSchemaEntry tableSchemaEntry = (TableSchemaEntry)source;
            table_catalog.Value = tableSchemaEntry.Catalog;
            table_schema.Value = tableSchemaEntry.Owner;
            table_name.Value = tableSchemaEntry.Name;
            table_type.Value = tableSchemaEntry.TableType;
        }

        [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.EF_Tables_FillRow")]
        public static IEnumerable EF_Tables_Query()
        {
            Database currentDatabase = GetCurrentDatabase();
            string databaseName = GetDatabaseName(currentDatabase);
            List<IVistaDBValue[]> vistaDbValueArrayList = new List<IVistaDBValue[]>(10);
            foreach (string key in (IEnumerable<string>)currentDatabase.GetTableIdMap().Keys)
            {
                IVistaDBValue[] vistaDbValueArray = new IVistaDBValue[4] { new VistaDBString("[dbo][" + key + "]"), new VistaDBString(databaseName), new VistaDBString("dbo"), new VistaDBString(key) };
                vistaDbValueArrayList.Add(vistaDbValueArray);
            }
            return vistaDbValueArrayList;
        }

        public static void EF_Tables_FillRow(object entry, out VistaDBString Id, out VistaDBString CatalogName, out VistaDBString SchemaName, out VistaDBString Name)
        {
            IVistaDBValue[] vistaDbValueArray = (IVistaDBValue[])entry;
            Id = (VistaDBString)vistaDbValueArray[0];
            CatalogName = (VistaDBString)vistaDbValueArray[1];
            SchemaName = (VistaDBString)vistaDbValueArray[2];
            Name = (VistaDBString)vistaDbValueArray[3];
        }

        private static void FillColumnInfo(IVistaDBColumnAttributes column, IVistaDBValue[] row, int offset)
        {
            FillColumnInfo(column.Name, column.RowIndex, column.AllowNull, column.Type, column.MaxLength, row, offset);
        }

        private static void FillColumnInfo(string name, int index, bool allowNull, VistaDBType type, int maxLength, IVistaDBValue[] row, int offset)
        {
            row[offset] = new VistaDBString(name);
            row[offset + 1] = new VistaDBInt32(index);
            row[offset + 2] = new VistaDBBoolean(allowNull);
            string lower = type.ToString().ToLower(CultureInfo.CurrentCulture);
            switch (type)
            {
                case VistaDBType.VarChar:
                case VistaDBType.NVarChar:
                case VistaDBType.VarBinary:
                    row[offset + 3] = maxLength < 1 || maxLength > 8000 ? new VistaDBString(lower + "(max)") : (IVistaDBValue)new VistaDBString(lower);
                    break;
                default:
                    row[offset + 3] = new VistaDBString(lower);
                    break;
            }
            switch (type)
            {
                case VistaDBType.Char:
                case VistaDBType.VarChar:
                    row[offset + 4] = new VistaDBInt32(maxLength);
                    break;
                case VistaDBType.NChar:
                case VistaDBType.NVarChar:
                    row[offset + 4] = new VistaDBInt32(maxLength);
                    break;
                case VistaDBType.Text:
                    row[offset + 4] = new VistaDBInt32(maxLength);
                    break;
                case VistaDBType.NText:
                    row[offset + 4] = new VistaDBInt32(maxLength);
                    break;
                case VistaDBType.Image:
                    row[offset + 4] = new VistaDBInt32(maxLength);
                    break;
                case VistaDBType.VarBinary:
                    row[offset + 4] = new VistaDBInt32(maxLength);
                    break;
                case VistaDBType.Timestamp:
                    row[offset + 4] = new VistaDBInt32(maxLength);
                    break;
                default:
                    row[offset + 4] = new VistaDBInt32();
                    break;
            }
            switch (type)
            {
                case VistaDBType.TinyInt:
                    row[offset + 5] = new VistaDBInt32(3);
                    row[offset + 6] = new VistaDBInt32();
                    row[offset + 7] = new VistaDBInt32(0);
                    break;
                case VistaDBType.SmallInt:
                    row[offset + 5] = new VistaDBInt32(5);
                    row[offset + 6] = new VistaDBInt32();
                    row[offset + 7] = new VistaDBInt32(0);
                    break;
                case VistaDBType.Int:
                    row[offset + 5] = new VistaDBInt32(10);
                    row[offset + 6] = new VistaDBInt32();
                    row[offset + 7] = new VistaDBInt32(0);
                    break;
                case VistaDBType.BigInt:
                    row[offset + 5] = new VistaDBInt32(19);
                    row[offset + 6] = new VistaDBInt32();
                    row[offset + 7] = new VistaDBInt32(0);
                    break;
                case VistaDBType.Real:
                    row[offset + 5] = new VistaDBInt32(7);
                    row[offset + 6] = new VistaDBInt32();
                    row[offset + 7] = new VistaDBInt32();
                    break;
                case VistaDBType.Float:
                    row[offset + 5] = new VistaDBInt32(15);
                    row[offset + 6] = new VistaDBInt32();
                    row[offset + 7] = new VistaDBInt32();
                    break;
                case VistaDBType.Decimal:
                    row[offset + 5] = new VistaDBInt32(18);
                    row[offset + 6] = new VistaDBInt32();
                    row[offset + 7] = new VistaDBInt32(0);
                    break;
                case VistaDBType.Money:
                    row[offset + 5] = new VistaDBInt32(19);
                    row[offset + 6] = new VistaDBInt32();
                    row[offset + 7] = new VistaDBInt32(4);
                    break;
                case VistaDBType.SmallMoney:
                    row[offset + 5] = new VistaDBInt32(10);
                    row[offset + 6] = new VistaDBInt32();
                    row[offset + 7] = new VistaDBInt32(4);
                    break;
                case VistaDBType.Bit:
                    row[offset + 5] = new VistaDBInt32(1);
                    row[offset + 6] = new VistaDBInt32();
                    row[offset + 7] = new VistaDBInt32();
                    break;
                case VistaDBType.DateTime:
                    row[offset + 5] = new VistaDBInt32(23);
                    row[offset + 6] = new VistaDBInt32(3);
                    row[offset + 7] = new VistaDBInt32(3);
                    break;
                case VistaDBType.UniqueIdentifier:
                    row[offset + 5] = new VistaDBInt32(36);
                    row[offset + 6] = new VistaDBInt32();
                    row[offset + 7] = new VistaDBInt32();
                    break;
                case VistaDBType.SmallDateTime:
                    row[offset + 5] = new VistaDBInt32(16);
                    row[offset + 6] = new VistaDBInt32();
                    row[offset + 7] = new VistaDBInt32(0);
                    break;
                case VistaDBType.Timestamp:
                    row[offset + 5] = new VistaDBInt32(8);
                    row[offset + 6] = new VistaDBInt32();
                    row[offset + 7] = new VistaDBInt32();
                    break;
                default:
                    row[offset + 5] = new VistaDBInt32();
                    row[offset + 6] = new VistaDBInt32();
                    row[offset + 7] = new VistaDBInt32();
                    break;
            }
            row[offset + 8] = new VistaDBString();
            row[offset + 9] = new VistaDBString();
            row[offset + 10] = new VistaDBString();
            row[offset + 11] = new VistaDBString();
            row[offset + 12] = new VistaDBString();
            row[offset + 13] = new VistaDBString();
        }

        [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.EF_TableColumns_FillRow")]
        public static IEnumerable EF_TableColumns_Query()
        {
            Database currentDatabase = GetCurrentDatabase();
            GetDatabaseName(currentDatabase);
            List<IVistaDBValue[]> vistaDbValueArrayList = new List<IVistaDBValue[]>();
            foreach (string key in (IEnumerable<string>)currentDatabase.GetTableIdMap().Keys)
            {
                using (IVistaDBTableSchema tableSchema = currentDatabase.GetTableSchema(key, false))
                {
                    foreach (IVistaDBColumnAttributes column in (IEnumerable<IVistaDBColumnAttributes>)tableSchema)
                    {
                        IVistaDBValue[] row = new IVistaDBValue[20];
                        row[0] = new VistaDBString("[dbo][" + key + "][" + column.Name + "]");
                        row[1] = new VistaDBString("[dbo][" + key + "]");
                        FillColumnInfo(column, row, 2);
                        row[16] = new VistaDBBoolean(false);
                        row[17] = tableSchema.Identities[column.Name] == null ? new VistaDBBoolean(false) : (IVistaDBValue)new VistaDBBoolean(true);
                        row[18] = column.Type != VistaDBType.Timestamp ? new VistaDBBoolean(false) : (IVistaDBValue)new VistaDBBoolean(true);
                        IVistaDBDefaultValueInformation defaultValue = tableSchema.DefaultValues[column.Name];
                        row[19] = defaultValue != null ? new VistaDBString(defaultValue.Expression) : (IVistaDBValue)new VistaDBString();
                        vistaDbValueArrayList.Add(row);
                    }
                }
            }
            return vistaDbValueArrayList;
        }

        public static void EF_TableColumns_FillRow(object entry, out VistaDBString Id, out VistaDBString ParentId, out VistaDBString Name, out VistaDBInt32 Ordinal, out VistaDBBoolean IsNullable, out VistaDBString TypeName, out VistaDBInt32 MaxLength, out VistaDBInt32 Precision, out VistaDBInt32 DateTimePrecision, out VistaDBInt32 Scale, out VistaDBString CollationCatalog, out VistaDBString CollationSchema, out VistaDBString CollationName, out VistaDBString CharacterSetCatalog, out VistaDBString CharacterSetSchema, out VistaDBString CharacterSetName, out VistaDBBoolean IsMultiSet, out VistaDBBoolean IsIdentity, out VistaDBBoolean IsStoreGenerated, out VistaDBString Default)
        {
            IVistaDBValue[] vistaDbValueArray = (IVistaDBValue[])entry;
            Id = (VistaDBString)vistaDbValueArray[0];
            ParentId = (VistaDBString)vistaDbValueArray[1];
            Name = (VistaDBString)vistaDbValueArray[2];
            Ordinal = (VistaDBInt32)vistaDbValueArray[3];
            IsNullable = (VistaDBBoolean)vistaDbValueArray[4];
            TypeName = (VistaDBString)vistaDbValueArray[5];
            MaxLength = (VistaDBInt32)vistaDbValueArray[6];
            Precision = (VistaDBInt32)vistaDbValueArray[7];
            DateTimePrecision = (VistaDBInt32)vistaDbValueArray[8];
            Scale = (VistaDBInt32)vistaDbValueArray[9];
            CollationCatalog = (VistaDBString)vistaDbValueArray[10];
            CollationSchema = (VistaDBString)vistaDbValueArray[11];
            CollationName = (VistaDBString)vistaDbValueArray[12];
            CharacterSetCatalog = (VistaDBString)vistaDbValueArray[13];
            CharacterSetSchema = (VistaDBString)vistaDbValueArray[14];
            CharacterSetName = (VistaDBString)vistaDbValueArray[15];
            IsMultiSet = (VistaDBBoolean)vistaDbValueArray[16];
            IsIdentity = (VistaDBBoolean)vistaDbValueArray[17];
            IsStoreGenerated = (VistaDBBoolean)vistaDbValueArray[18];
            Default = (VistaDBString)vistaDbValueArray[19];
        }

        [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillColumnSchema")]
        public static IEnumerable ColumnSchema()
        {
            List<ColumnSchemaEntry> columnSchemaEntryList = new List<ColumnSchemaEntry>();
            Database currentDatabase = GetCurrentDatabase();
            string databaseName = GetDatabaseName(currentDatabase);
            foreach (string key in (IEnumerable<string>)currentDatabase.GetTableIdMap().Keys)
            {
                IVistaDBTableSchema tableSchema = currentDatabase.GetTableSchema(key, false);
                foreach (IVistaDBColumnAttributes schema in (IEnumerable<IVistaDBColumnAttributes>)tableSchema)
                {
                    IVistaDBDefaultValueInformation defaultValue = tableSchema.DefaultValues[schema.Name];
                    IVistaDBIdentityInformation identity = tableSchema.Identities[schema.Name];
                    columnSchemaEntryList.Add(new ColumnSchemaEntry(databaseName, tableSchema, schema, defaultValue, identity));
                }
            }
            foreach (DataRow row in (InternalDataCollectionBase)new VistaDBConnection(VistaDBContext.DDAChannel.CurrentDatabase).GetSchema("VIEWCOLUMNS").Rows)
                columnSchemaEntryList.Add(new ColumnSchemaEntry(databaseName, row));
            return columnSchemaEntryList;
        }

        public static void FillColumnSchema(object source, VistaDBString table_catalog, VistaDBString table_schema, VistaDBString table_name, VistaDBString column_name, VistaDBInt32 ordinal_position, VistaDBString column_default, VistaDBString is_nullable, VistaDBString data_type, VistaDBInt32 character_maximum_length, VistaDBInt32 character_octet_length, VistaDBByte numeric_precision, VistaDBInt16 numeric_precision_radix, VistaDBInt16 numeric_scale, VistaDBInt16 datetime_precision, VistaDBString character_set_catalog, VistaDBString character_set_schema, VistaDBString character_set_name, VistaDBString collation_catalog, VistaDBString collation_schema, VistaDBString collation_name, VistaDBString domain_catalog, VistaDBString domain_schema, VistaDBString domain_name, VistaDBBoolean is_identity, VistaDBBoolean is_storegenerated)
        {
            ColumnSchemaEntry columnSchemaEntry = (ColumnSchemaEntry)source;
            table_catalog.Value = columnSchemaEntry.Catalog;
            table_schema.Value = columnSchemaEntry.Owner;
            table_name.Value = columnSchemaEntry.Table;
            column_name.Value = columnSchemaEntry.Name;
            ordinal_position.Value = columnSchemaEntry.Index;
            column_default.Value = columnSchemaEntry.Default;
            is_nullable.Value = columnSchemaEntry.Nullable ? "YES" : (object)"NO";
            data_type.Value = columnSchemaEntry.DataType.ToString().ToLower();
            is_identity.Value = columnSchemaEntry.IsIdentity;
            character_maximum_length.Value = null;
            character_octet_length.Value = null;
            numeric_precision.Value = null;
            numeric_precision_radix.Value = null;
            numeric_scale.Value = null;
            datetime_precision.Value = null;
            if (columnSchemaEntry.DateTimeSub != 0)
                datetime_precision.Value = columnSchemaEntry.DateTimeSub;
            else if (columnSchemaEntry.Radix != 0)
            {
                numeric_precision.Value = (byte)columnSchemaEntry.Precision;
                numeric_precision_radix.Value = columnSchemaEntry.Radix;
                numeric_scale.Value = columnSchemaEntry.Scale;
            }
            else if (columnSchemaEntry.MaxLength != 0)
            {
                character_maximum_length.Value = columnSchemaEntry.Precision;
                character_octet_length.Value = columnSchemaEntry.OctetLength;
            }
            character_set_catalog.Value = null;
            character_set_schema.Value = null;
            character_set_name.Value = null;
            collation_catalog.Value = null;
            collation_schema.Value = null;
            collation_name.Value = null;
            domain_catalog.Value = null;
            domain_schema.Value = null;
            domain_name.Value = null;
        }

        [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.EF_Views_FillRow")]
        public static IEnumerable EF_Views_Query()
        {
            Database currentDatabase = GetCurrentDatabase();
            string databaseName = GetDatabaseName(currentDatabase);
            List<IVistaDBValue[]> vistaDbValueArrayList = new List<IVistaDBValue[]>();
            foreach (Database.ViewList.View view in (IEnumerable)currentDatabase.LoadViews().Values)
            {
                IVistaDBValue[] vistaDbValueArray = new IVistaDBValue[6] { new VistaDBString("[dbo][" + view.Name + "]"), new VistaDBString(databaseName), new VistaDBString("dbo"), new VistaDBString(view.Name), new VistaDBString(view.Expression), new VistaDBBoolean(false) };
                vistaDbValueArrayList.Add(vistaDbValueArray);
            }
            return vistaDbValueArrayList;
        }

        public static void EF_Views_FillRow(object entry, out VistaDBString Id, out VistaDBString CatalogName, out VistaDBString SchemaName, out VistaDBString Name, out VistaDBString ViewDefinition, out VistaDBBoolean IsUpdatable)
        {
            IVistaDBValue[] vistaDbValueArray = (IVistaDBValue[])entry;
            Id = (VistaDBString)vistaDbValueArray[0];
            CatalogName = (VistaDBString)vistaDbValueArray[1];
            SchemaName = (VistaDBString)vistaDbValueArray[2];
            Name = (VistaDBString)vistaDbValueArray[3];
            ViewDefinition = (VistaDBString)vistaDbValueArray[4];
            IsUpdatable = (VistaDBBoolean)vistaDbValueArray[5];
        }

        [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.EF_ViewColumns_FillRow")]
        public static IEnumerable EF_ViewColumns_Query()
        {
            Database currentDatabase = GetCurrentDatabase();
            GetDatabaseName(currentDatabase);
            LocalSQLConnection currentConnection = (LocalSQLConnection)VistaDBContext.SQLChannel.CurrentConnection;
            List<IVistaDBValue[]> vistaDbValueArrayList = new List<IVistaDBValue[]>();
            foreach (Database.ViewList.View view in (IEnumerable)currentDatabase.LoadViews().Values)
            {
                CreateViewStatement createViewStatement = null;
                try
                {
                    Statement statement = (Statement)currentConnection.CreateBatchStatement(view.Expression, 0L).SubQuery(0);
                    createViewStatement = statement as CreateViewStatement;
                    if (createViewStatement != null)
                    {
                        int num1 = (int)statement.PrepareQuery();
                        SelectStatement selectStatement = ((CreateViewStatement)statement).SelectStatement;
                        int num2 = 0;
                        for (int columnCount = selectStatement.ColumnCount; num2 < columnCount; ++num2)
                        {
                            IVistaDBValue[] row = new IVistaDBValue[20];
                            string aliasName = selectStatement.GetAliasName(num2);
                            row[0] = new VistaDBString("[dbo][" + view.Name + "][" + aliasName + "]");
                            row[1] = new VistaDBString("[dbo][" + view.Name + "]");
                            VistaDBType columnVistaDbType = selectStatement.GetColumnVistaDBType(num2);
                            FillColumnInfo(aliasName, num2, selectStatement.GetIsAllowNull(num2), columnVistaDbType, selectStatement.GetWidth(num2), row, 2);
                            row[16] = new VistaDBBoolean(false);
                            row[17] = new VistaDBBoolean(selectStatement.GetIsAutoIncrement(num2));
                            row[18] = columnVistaDbType != VistaDBType.Timestamp ? new VistaDBBoolean(false) : (IVistaDBValue)new VistaDBBoolean(true);
                            bool useInUpdate;
                            row[19] = new VistaDBString(selectStatement.GetDefaultValue(num2, out useInUpdate));
                            vistaDbValueArrayList.Add(row);
                        }
                        createViewStatement.DropTemporaryTables();
                    }
                }
                catch (Exception)
                {
                }
                finally
                {
                    createViewStatement?.DropTemporaryTables();
                }
            }
            return vistaDbValueArrayList;
        }

        public static void EF_ViewColumns_FillRow(object entry, out VistaDBString Id, out VistaDBString ParentId, out VistaDBString Name, out VistaDBInt32 Ordinal, out VistaDBBoolean IsNullable, out VistaDBString TypeName, out VistaDBInt32 MaxLength, out VistaDBInt32 Precision, out VistaDBInt32 DateTimePrecision, out VistaDBInt32 Scale, out VistaDBString CollationCatalog, out VistaDBString CollationSchema, out VistaDBString CollationName, out VistaDBString CharacterSetCatalog, out VistaDBString CharacterSetSchema, out VistaDBString CharacterSetName, out VistaDBBoolean IsMultiSet, out VistaDBBoolean IsIdentity, out VistaDBBoolean IsStoreGenerated, out VistaDBString Default)
        {
            IVistaDBValue[] vistaDbValueArray = (IVistaDBValue[])entry;
            Id = (VistaDBString)vistaDbValueArray[0];
            ParentId = (VistaDBString)vistaDbValueArray[1];
            Name = (VistaDBString)vistaDbValueArray[2];
            Ordinal = (VistaDBInt32)vistaDbValueArray[3];
            IsNullable = (VistaDBBoolean)vistaDbValueArray[4];
            TypeName = (VistaDBString)vistaDbValueArray[5];
            MaxLength = (VistaDBInt32)vistaDbValueArray[6];
            Precision = (VistaDBInt32)vistaDbValueArray[7];
            DateTimePrecision = (VistaDBInt32)vistaDbValueArray[8];
            Scale = (VistaDBInt32)vistaDbValueArray[9];
            CollationCatalog = (VistaDBString)vistaDbValueArray[10];
            CollationSchema = (VistaDBString)vistaDbValueArray[11];
            CollationName = (VistaDBString)vistaDbValueArray[12];
            CharacterSetCatalog = (VistaDBString)vistaDbValueArray[13];
            CharacterSetSchema = (VistaDBString)vistaDbValueArray[14];
            CharacterSetName = (VistaDBString)vistaDbValueArray[15];
            IsMultiSet = (VistaDBBoolean)vistaDbValueArray[16];
            IsIdentity = (VistaDBBoolean)vistaDbValueArray[17];
            IsStoreGenerated = (VistaDBBoolean)vistaDbValueArray[18];
            Default = (VistaDBString)vistaDbValueArray[19];
        }

        [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillViewSchema")]
        public static IEnumerable ViewSchema()
        {
            List<ViewSchemaEntry> viewSchemaEntryList = new List<ViewSchemaEntry>();
            Database currentDatabase = GetCurrentDatabase();
            string databaseName = GetDatabaseName(currentDatabase);
            foreach (Database.ViewList.View schema in (IEnumerable)currentDatabase.LoadViews().Values)
                viewSchemaEntryList.Add(new ViewSchemaEntry(databaseName, schema));
            return viewSchemaEntryList;
        }

        public static void FillViewSchema(object source, VistaDBString table_catalog, VistaDBString table_schema, VistaDBString table_name, VistaDBString view_definition, VistaDBString check_option, VistaDBString is_updatable)
        {
            ViewSchemaEntry viewSchemaEntry = (ViewSchemaEntry)source;
            table_catalog.Value = viewSchemaEntry.Catalog;
            table_schema.Value = viewSchemaEntry.Owner;
            table_name.Value = viewSchemaEntry.Name;
            view_definition.Value = viewSchemaEntry.Expression;
            check_option.Value = "NONE";
            is_updatable.Value = "NO";
        }

        [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillViewColumnSchema")]
        public static IEnumerable ViewColumnSchema()
        {
            List<ViewColumnDataRow> viewColumnDataRowList = new List<ViewColumnDataRow>();
            VistaDBConnection currentConnection = GetCurrentConnection();
            string databaseName = GetDatabaseName(currentConnection.Database);
            foreach (DataRow row in (InternalDataCollectionBase)currentConnection.GetSchema("VIEWCOLUMNS").Rows)
                viewColumnDataRowList.Add(new ViewColumnDataRow(databaseName, row));
            return viewColumnDataRowList;
        }

        public static void FillViewColumnSchema(object source, VistaDBString view_catalog, VistaDBString view_schema, VistaDBString view_name, VistaDBString table_catalog, VistaDBString table_schema, VistaDBString table_name, VistaDBString column_name)
        {
            ViewColumnDataRow viewColumnDataRow = source as ViewColumnDataRow;
            view_catalog.Value = viewColumnDataRow.ViewCatalog;
            view_schema.Value = viewColumnDataRow.ViewSchema;
            view_name.Value = viewColumnDataRow.ViewName;
            table_catalog.Value = viewColumnDataRow.TableCatalog;
            table_schema.Value = viewColumnDataRow.TableSchema;
            table_name.Value = viewColumnDataRow.TableName;
            column_name.Value = viewColumnDataRow.Name;
        }

        [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillViewTableSchema")]
        public static IEnumerable ViewTableSchema()
        {
            return new List<object>();
        }

        public static void FillViewTableSchema(object source, VistaDBString view_catalog, VistaDBString view_schema, VistaDBString view_name, VistaDBString table_catalog, VistaDBString table_schema, VistaDBString table_name)
        {
        }

        [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.EF_Constraints_FillRow")]
        public static IEnumerable EF_Constraints_Query()
        {
            Database currentDatabase = GetCurrentDatabase();
            GetDatabaseName(currentDatabase);
            LocalSQLConnection currentConnection = (LocalSQLConnection)VistaDBContext.SQLChannel.CurrentConnection;
            List<IVistaDBValue[]> vistaDbValueArrayList = new List<IVistaDBValue[]>(10);
            foreach (KeyValuePair<ulong, string> tableId in currentDatabase.GetTableIdMap())
            {
                IVistaDBIndexCollection indexes = new Table.TableSchema.IndexCollection();
                currentDatabase.GetIndexes(tableId.Key, indexes);
                foreach (IVistaDBIndexInformation indexInformation in (IEnumerable<IVistaDBIndexInformation>)indexes.Values)
                {
                    if (indexInformation.Unique || indexInformation.Primary || indexInformation.FKConstraint)
                    {
                        IVistaDBValue[] vistaDbValueArray = new IVistaDBValue[6] { new VistaDBString("[dbo][" + tableId.Value + "][" + indexInformation.Name + "]"), new VistaDBString("[dbo][" + tableId.Value + "]"), new VistaDBString(indexInformation.Name), null, null, null };
                        string val = null;
                        if (indexInformation.Primary)
                            val = "PRIMARY KEY";
                        else if (indexInformation.Unique)
                            val = "UNIQUE";
                        else if (indexInformation.FKConstraint)
                            val = "FOREIGN KEY";
                        vistaDbValueArray[3] = new VistaDBString(val);
                        vistaDbValueArray[4] = new VistaDBBoolean(false);
                        vistaDbValueArray[5] = new VistaDBBoolean(false);
                        vistaDbValueArrayList.Add(vistaDbValueArray);
                    }
                }
            }
            return vistaDbValueArrayList;
        }

        public static void EF_Constraints_FillRow(object entry, out VistaDBString Id, out VistaDBString ParentId, out VistaDBString Name, out VistaDBString ConstraintType, out VistaDBBoolean IsDeferrable, out VistaDBBoolean IsInitiallyDeferred)
        {
            IVistaDBValue[] vistaDbValueArray = (IVistaDBValue[])entry;
            Id = (VistaDBString)vistaDbValueArray[0];
            ParentId = (VistaDBString)vistaDbValueArray[1];
            Name = (VistaDBString)vistaDbValueArray[2];
            ConstraintType = (VistaDBString)vistaDbValueArray[3];
            IsDeferrable = (VistaDBBoolean)vistaDbValueArray[4];
            IsInitiallyDeferred = (VistaDBBoolean)vistaDbValueArray[5];
        }

        [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillTableConstraintSchema")]
        public static IEnumerable TableConstraintSchema()
        {
            List<RelationshipSchemaEntry> relationshipSchemaEntryList = new List<RelationshipSchemaEntry>();
            Database currentDatabase = GetCurrentDatabase();
            string databaseName = GetDatabaseName(currentDatabase);
            foreach (string key in (IEnumerable<string>)currentDatabase.GetTableIdMap().Keys)
            {
                IVistaDBTableSchema tableSchema = currentDatabase.GetTableSchema(key, true);
                foreach (IVistaDBIndexInformation index in (IEnumerable<IVistaDBIndexInformation>)tableSchema.Indexes.Values)
                {
                    if (index.Unique || index.Primary || index.FKConstraint)
                        relationshipSchemaEntryList.Add(new RelationshipSchemaEntry(databaseName, tableSchema.Name, index));
                }
                foreach (IVistaDBConstraintInformation constraint in (IEnumerable<IVistaDBConstraintInformation>)tableSchema.Constraints.Values)
                    relationshipSchemaEntryList.Add(new RelationshipSchemaEntry(databaseName, tableSchema.Name, constraint));
            }
            return relationshipSchemaEntryList;
        }

        public static void FillTableConstraintSchema(object source, VistaDBString constraint_catalog, VistaDBString constraint_schema, VistaDBString constraint_name, VistaDBString table_catalog, VistaDBString table_schema, VistaDBString table_name, VistaDBString constraint_type, VistaDBString is_deferrable, VistaDBString initially_deferred)
        {
            RelationshipSchemaEntry relationshipSchemaEntry = (RelationshipSchemaEntry)source;
            constraint_catalog.Value = relationshipSchemaEntry.Catalog;
            constraint_schema.Value = relationshipSchemaEntry.Owner;
            constraint_name.Value = relationshipSchemaEntry.Name;
            table_catalog.Value = relationshipSchemaEntry.Catalog;
            table_schema.Value = relationshipSchemaEntry.Owner;
            table_name.Value = relationshipSchemaEntry.Table;
            constraint_type.Value = relationshipSchemaEntry.RelationType;
            is_deferrable.Value = "NO";
            initially_deferred.Value = "NO";
        }

        [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.EF_ForeignKeyConstraints_FillRow")]
        public static IEnumerable EF_ForeignKeyConstraints_Query()
        {
            Database currentDatabase = GetCurrentDatabase();
            GetDatabaseName(currentDatabase);
            List<IVistaDBValue[]> vistaDbValueArrayList = new List<IVistaDBValue[]>(10);
            foreach (IVistaDBRelationshipInformation relationshipInformation in (IEnumerable<IVistaDBRelationshipInformation>)currentDatabase.GetRelationships().Values)
            {
                IVistaDBValue[] vistaDbValueArray = new IVistaDBValue[3] { new VistaDBString("[dbo][" + relationshipInformation.ForeignTable + "][" + relationshipInformation.Name + "]"), new VistaDBString(GetReferentialIntegrity(relationshipInformation.UpdateIntegrity)), new VistaDBString(GetReferentialIntegrity(relationshipInformation.DeleteIntegrity)) };
                vistaDbValueArrayList.Add(vistaDbValueArray);
            }
            return vistaDbValueArrayList;
        }

        public static void EF_ForeignKeyConstraints_FillRow(object entry, out VistaDBString Id, out VistaDBString UpdateRule, out VistaDBString DeleteRule)
        {
            IVistaDBValue[] vistaDbValueArray = (IVistaDBValue[])entry;
            Id = (VistaDBString)vistaDbValueArray[0];
            UpdateRule = (VistaDBString)vistaDbValueArray[1];
            DeleteRule = (VistaDBString)vistaDbValueArray[2];
        }

        [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.EF_ForeignKeys_FillRow")]
        public static IEnumerable EF_ForeignKeys_Query()
        {
            Database currentDatabase = GetCurrentDatabase();
            GetDatabaseName(currentDatabase);
            List<IVistaDBValue[]> vistaDbValueArrayList = new List<IVistaDBValue[]>(10);
            Dictionary<string, IVistaDBTableSchema> dictionary1 = new Dictionary<string, IVistaDBTableSchema>();
            Dictionary<string, IVistaDBIndexInformation> dictionary2 = new Dictionary<string, IVistaDBIndexInformation>();
            foreach (IVistaDBRelationshipInformation relationshipInformation in (IEnumerable<IVistaDBRelationshipInformation>)currentDatabase.GetRelationships().Values)
            {
                int val = 0;
                IVistaDBTableSchema tableSchema1;
                if (!dictionary1.TryGetValue(relationshipInformation.ForeignTable, out tableSchema1))
                {
                    tableSchema1 = currentDatabase.GetTableSchema(relationshipInformation.ForeignTable, false);
                    dictionary1.Add(tableSchema1.Name, tableSchema1);
                }
                IVistaDBTableSchema tableSchema2;
                if (!dictionary1.TryGetValue(relationshipInformation.PrimaryTable, out tableSchema2))
                {
                    tableSchema2 = currentDatabase.GetTableSchema(relationshipInformation.PrimaryTable, false);
                    dictionary1.Add(tableSchema2.Name, tableSchema2);
                }
                IVistaDBIndexInformation indexInformation1 = null;
                if (!dictionary2.TryGetValue(tableSchema2.Name, out indexInformation1))
                {
                    foreach (IVistaDBIndexInformation indexInformation2 in (IEnumerable<IVistaDBIndexInformation>)tableSchema2.Indexes.Values)
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
                    IVistaDBValue[] vistaDbValueArray = new IVistaDBValue[5] { new VistaDBString("[dbo][" + relationshipInformation.ForeignTable + "][" + relationshipInformation.Name + "][" + val.ToString() + "]"), new VistaDBString("[dbo][" + relationshipInformation.PrimaryTable + "][" + tableSchema2[indexInformation1.KeyStructure[index].RowIndex].Name + "]"), new VistaDBString("[dbo][" + relationshipInformation.ForeignTable + "][" + strArray[index] + "]"), new VistaDBString("[dbo][" + relationshipInformation.ForeignTable + "][" + relationshipInformation.Name + "]"), new VistaDBInt32(val) };
                    vistaDbValueArrayList.Add(vistaDbValueArray);
                }
            }
            return vistaDbValueArrayList;
        }

        public static void EF_ForeignKeys_FillRow(object entry, out VistaDBString Id, out VistaDBString ToColumnId, out VistaDBString FromColumnId, out VistaDBString ConstraintId, out VistaDBInt32 Ordinal)
        {
            IVistaDBValue[] vistaDbValueArray = (IVistaDBValue[])entry;
            Id = (VistaDBString)vistaDbValueArray[0];
            ToColumnId = (VistaDBString)vistaDbValueArray[1];
            FromColumnId = (VistaDBString)vistaDbValueArray[2];
            ConstraintId = (VistaDBString)vistaDbValueArray[3];
            Ordinal = (VistaDBInt32)vistaDbValueArray[4];
        }

        [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.EF_ConstraintColumns_FillRow")]
        public static IEnumerable EF_ConstraintColumns_Query()
        {
            Database currentDatabase = GetCurrentDatabase();
            GetDatabaseName(currentDatabase);
            List<IVistaDBValue[]> vistaDbValueArrayList = new List<IVistaDBValue[]>(10);
            foreach (KeyValuePair<ulong, string> tableId in currentDatabase.GetTableIdMap())
            {
                Table.TableSchema.IndexCollection indexCollection = new Table.TableSchema.IndexCollection();
                currentDatabase.GetIndexes(tableId.Key, indexCollection);
                foreach (IVistaDBIndexInformation indexInformation in indexCollection.Values)
                {
                    if (indexInformation.Unique || indexInformation.Primary || indexInformation.FKConstraint)
                    {
                        Row row = currentDatabase.AllocateRowsetSchema(tableId.Key, currentDatabase.CreateEmptyRowInstance());
                        foreach (IVistaDBKeyColumn vistaDbKeyColumn in indexInformation.KeyStructure)
                        {
                            IVistaDBValue[] vistaDbValueArray = new IVistaDBValue[2] { new VistaDBString("[dbo][" + tableId.Value + "][" + indexInformation.Name + "]"), new VistaDBString("[dbo][" + tableId.Value + "][" + row[vistaDbKeyColumn.RowIndex].Name + "]") };
                            vistaDbValueArrayList.Add(vistaDbValueArray);
                        }
                    }
                }
            }
            return vistaDbValueArrayList;
        }

        public static void EF_ConstraintColumns_FillRow(object entry, out VistaDBString ConstraintId, out VistaDBString ColumnId)
        {
            IVistaDBValue[] vistaDbValueArray = (IVistaDBValue[])entry;
            ConstraintId = (VistaDBString)vistaDbValueArray[0];
            ColumnId = (VistaDBString)vistaDbValueArray[1];
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
            List<RelationshipSchemaEntry> relationshipSchemaEntryList = new List<RelationshipSchemaEntry>();
            Database currentDatabase = GetCurrentDatabase();
            string databaseName = GetDatabaseName(currentDatabase);
            foreach (string key in (IEnumerable<string>)currentDatabase.GetTableIdMap().Keys)
            {
                IVistaDBTableSchema tableSchema = currentDatabase.GetTableSchema(key, true);
                foreach (IVistaDBIndexInformation indexInformation1 in (IEnumerable<IVistaDBIndexInformation>)tableSchema.Indexes.Values)
                {
                    if (indexInformation1.FKConstraint)
                    {
                        IVistaDBRelationshipInformation foreignKey1 = tableSchema.ForeignKeys[indexInformation1.Name];
                        IVistaDBTableSchema primaryTable = currentDatabase.GetTableSchema(foreignKey1.PrimaryTable, true);
                        IVistaDBIndexInformation foreign_index = null;
                        foreach (IVistaDBIndexInformation indexInformation2 in (IEnumerable<IVistaDBIndexInformation>)primaryTable.Indexes.Values)
                        {
                            if (indexInformation2.Primary)
                            {
                                bool flag = false;
                                string foreignKey2 = foreignKey1.ForeignKey;
                                char[] chArray = new char[1] { ';' };
                                foreach (string str in foreignKey2.Split(chArray))
                                {
                                    string foreignKey = str;
                                    flag = Array.Exists(indexInformation2.KeyStructure, matchKey => string.Compare(primaryTable[matchKey.RowIndex].Name, foreignKey, StringComparison.OrdinalIgnoreCase) == 0);
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
                            relationshipSchemaEntryList.Add(new RelationshipSchemaEntry(databaseName, foreignKey1, foreign_index));
                    }
                }
            }
            return relationshipSchemaEntryList;
        }

        public static void FillReferentialConstraintSchema(object source, VistaDBString constraint_catalog, VistaDBString constraint_schema, VistaDBString constraint_name, VistaDBString unique_constraint_catalog, VistaDBString unique_constraint_schema, VistaDBString unique_constraint_name, VistaDBString match_option, VistaDBString update_rule, VistaDBString delete_rule)
        {
            RelationshipSchemaEntry relationshipSchemaEntry = (RelationshipSchemaEntry)source;
            constraint_catalog.Value = relationshipSchemaEntry.Catalog;
            constraint_schema.Value = relationshipSchemaEntry.Owner;
            constraint_name.Value = relationshipSchemaEntry.Name;
            unique_constraint_catalog.Value = relationshipSchemaEntry.Catalog;
            unique_constraint_schema.Value = relationshipSchemaEntry.Owner;
            unique_constraint_name.Value = relationshipSchemaEntry.Expression;
            match_option.Value = "SIMPLE";
            update_rule.Value = relationshipSchemaEntry.UpdateRule;
            delete_rule.Value = relationshipSchemaEntry.DeleteRule;
        }

        [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillKeyColumnUsageSchema")]
        public static IEnumerable KeyColumnUsageSchema()
        {
            List<RelationshipSchemaEntry> relationshipSchemaEntryList = new List<RelationshipSchemaEntry>();
            Database currentDatabase = GetCurrentDatabase();
            string databaseName = GetDatabaseName(currentDatabase);
            foreach (string key1 in (IEnumerable<string>)currentDatabase.GetTableIdMap().Keys)
            {
                IVistaDBTableSchema tableSchema1 = currentDatabase.GetTableSchema(key1, true);
                foreach (IVistaDBIndexInformation index1 in (IEnumerable<IVistaDBIndexInformation>)tableSchema1.Indexes.Values)
                {
                    if (index1.Primary || index1.Unique)
                    {
                        foreach (IVistaDBKeyColumn key2 in index1.KeyStructure)
                            relationshipSchemaEntryList.Add(new RelationshipSchemaEntry(databaseName, tableSchema1, index1, tableSchema1, key2));
                    }
                    else if (index1.FKConstraint)
                    {
                        IVistaDBRelationshipInformation foreignKey = tableSchema1.ForeignKeys[index1.Name];
                        IVistaDBTableSchema tableSchema2 = currentDatabase.GetTableSchema(foreignKey.PrimaryTable, true);
                        IVistaDBIndexInformation indexInformation1 = null;
                        foreach (IVistaDBIndexInformation indexInformation2 in (IEnumerable<IVistaDBIndexInformation>)tableSchema2.Indexes.Values)
                        {
                            if (indexInformation2.Primary)
                            {
                                indexInformation1 = indexInformation2;
                                break;
                            }
                        }
                        for (int index2 = 0; index2 < index1.KeyStructure.Length; ++index2)
                            relationshipSchemaEntryList.Add(new RelationshipSchemaEntry(databaseName, tableSchema1, index1, tableSchema2, indexInformation1.KeyStructure[index2]));
                    }
                }
            }
            return relationshipSchemaEntryList;
        }

        public static void FillKeyColumnUsageSchema(object source, VistaDBString constraint_catalog, VistaDBString constraint_schema, VistaDBString constraint_name, VistaDBString table_catalog, VistaDBString table_schema, VistaDBString table_name, VistaDBString column_name, VistaDBInt32 ordinal_position)
        {
            RelationshipSchemaEntry relationshipSchemaEntry = (RelationshipSchemaEntry)source;
            constraint_catalog.Value = relationshipSchemaEntry.Catalog;
            constraint_schema.Value = relationshipSchemaEntry.Owner;
            constraint_name.Value = relationshipSchemaEntry.Name;
            table_catalog.Value = relationshipSchemaEntry.Catalog;
            table_schema.Value = relationshipSchemaEntry.Owner;
            table_name.Value = relationshipSchemaEntry.Table;
            column_name.Value = relationshipSchemaEntry.Expression;
            ordinal_position.Value = relationshipSchemaEntry.ColumnOrdinal;
        }

        [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillCheckConstraintSchema")]
        public static IEnumerable CheckConstraintSchema()
        {
            List<RelationshipSchemaEntry> relationshipSchemaEntryList = new List<RelationshipSchemaEntry>();
            Database currentDatabase = GetCurrentDatabase();
            string databaseName = GetDatabaseName(currentDatabase);
            foreach (string key in (IEnumerable<string>)currentDatabase.GetTableIdMap().Keys)
            {
                IVistaDBTableSchema tableSchema = currentDatabase.GetTableSchema(key, true);
                foreach (IVistaDBConstraintInformation constraint in (IEnumerable<IVistaDBConstraintInformation>)tableSchema.Constraints.Values)
                    relationshipSchemaEntryList.Add(new RelationshipSchemaEntry(databaseName, tableSchema.Name, constraint));
            }
            return relationshipSchemaEntryList;
        }

        public static void FillCheckConstraintSchema(object source, VistaDBString constraint_catalog, VistaDBString constraint_schema, VistaDBString constraint_name, VistaDBString check_clause)
        {
            RelationshipSchemaEntry relationshipSchemaEntry = (RelationshipSchemaEntry)source;
            constraint_catalog.Value = relationshipSchemaEntry.Catalog;
            constraint_schema.Value = relationshipSchemaEntry.Owner;
            constraint_name.Value = relationshipSchemaEntry.Name;
            check_clause.Value = relationshipSchemaEntry.Expression;
        }

        [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillForeignKeySchema")]
        public static IEnumerable ForeignKeySchema()
        {
            List<ForeignKeySchemaEntry> foreignKeySchemaEntryList = new List<ForeignKeySchemaEntry>();
            Database currentDatabase = GetCurrentDatabase();
            string databaseName = GetDatabaseName(currentDatabase);
            foreach (string key in (IEnumerable<string>)currentDatabase.GetTableIdMap().Keys)
            {
                IVistaDBTableSchema tableSchema = currentDatabase.GetTableSchema(key, false);
                foreach (IVistaDBIndexInformation index in (IEnumerable<IVistaDBIndexInformation>)tableSchema.Indexes.Values)
                {
                    if (index.FKConstraint)
                        foreignKeySchemaEntryList.Add(new ForeignKeySchemaEntry(databaseName, tableSchema, index, tableSchema.ForeignKeys[index.Name]));
                }
            }
            return foreignKeySchemaEntryList;
        }

        public static void FillForeignKeySchema(object source, VistaDBString foreign_key_catalog, VistaDBString foreign_key_schema, VistaDBString foreign_key_name, VistaDBString foreign_key_table, VistaDBString table_name, VistaDBString table_schema, VistaDBInt32 update_referential_action, VistaDBInt32 delete_referential_action)
        {
            ForeignKeySchemaEntry foreignKeySchemaEntry = (ForeignKeySchemaEntry)source;
            foreign_key_catalog.Value = foreignKeySchemaEntry.Catalog;
            foreign_key_schema.Value = foreignKeySchemaEntry.Owner;
            foreign_key_name.Value = foreignKeySchemaEntry.Name;
            foreign_key_table.Value = foreignKeySchemaEntry.Table;
            table_name.Value = foreignKeySchemaEntry.TargetTable;
            table_schema.Value = foreignKeySchemaEntry.Owner;
            update_referential_action.Value = foreignKeySchemaEntry.Update;
            delete_referential_action.Value = foreignKeySchemaEntry.Delete;
        }

        [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillForeignKeyColumnSchema")]
        public static IEnumerable ForeignKeyColumnSchema()
        {
            List<ForeignKeyColumnSchemaEntry> columnSchemaEntryList = new List<ForeignKeyColumnSchemaEntry>();
            Database currentDatabase = GetCurrentDatabase();
            string databaseName = GetDatabaseName(currentDatabase);
            foreach (string key in (IEnumerable<string>)currentDatabase.GetTableIdMap().Keys)
            {
                IVistaDBTableSchema tableSchema1 = currentDatabase.GetTableSchema(key, false);
                foreach (IVistaDBIndexInformation index in (IEnumerable<IVistaDBIndexInformation>)tableSchema1.Indexes.Values)
                {
                    if (index.FKConstraint)
                    {
                        IVistaDBRelationshipInformation foreignKey = tableSchema1.ForeignKeys[index.Name];
                        IVistaDBTableSchema tableSchema2 = currentDatabase.GetTableSchema(foreignKey.PrimaryTable, true);
                        IVistaDBIndexInformation targetKey = null;
                        foreach (IVistaDBIndexInformation indexInformation in (IEnumerable<IVistaDBIndexInformation>)tableSchema2.Indexes.Values)
                        {
                            if (indexInformation.Primary)
                            {
                                targetKey = indexInformation;
                                break;
                            }
                        }
                        foreach (IVistaDBKeyColumn column in index.KeyStructure)
                            columnSchemaEntryList.Add(new ForeignKeyColumnSchemaEntry(databaseName, tableSchema1, index, foreignKey, tableSchema2, column, targetKey));
                    }
                }
            }
            return columnSchemaEntryList;
        }

        public static void FillForeignKeyColumnSchema(object source, VistaDBString foreign_key_catalog, VistaDBString foreign_key_schema, VistaDBString foreign_key_name, VistaDBString foreign_key_table, VistaDBString foreign_key_column, VistaDBString column_name, VistaDBInt32 ordinal_position)
        {
            ForeignKeyColumnSchemaEntry columnSchemaEntry = (ForeignKeyColumnSchemaEntry)source;
            foreign_key_catalog.Value = columnSchemaEntry.Catalog;
            foreign_key_schema.Value = columnSchemaEntry.Owner;
            foreign_key_name.Value = columnSchemaEntry.Name;
            foreign_key_table.Value = columnSchemaEntry.Table;
            foreign_key_column.Value = columnSchemaEntry.Key;
            column_name.Value = columnSchemaEntry.Column;
            ordinal_position.Value = columnSchemaEntry.Ordinal;
        }

        [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillIndexSchema")]
        public static IEnumerable IndexSchema()
        {
            List<IndexSchemaEntry> indexSchemaEntryList = new List<IndexSchemaEntry>();
            Database currentDatabase = GetCurrentDatabase();
            string databaseName = GetDatabaseName(currentDatabase);
            foreach (string key in (IEnumerable<string>)currentDatabase.GetTableIdMap().Keys)
            {
                IVistaDBTableSchema tableSchema = currentDatabase.GetTableSchema(key, false);
                foreach (IVistaDBIndexInformation index in (IEnumerable<IVistaDBIndexInformation>)tableSchema.Indexes.Values)
                {
                    if (!index.FKConstraint)
                        indexSchemaEntryList.Add(new IndexSchemaEntry(databaseName, tableSchema, index));
                }
            }
            return indexSchemaEntryList;
        }

        public static void FillIndexSchema(object source, VistaDBString index_catalog, VistaDBString index_schema, VistaDBString index_name, VistaDBString table_name, VistaDBBoolean is_primary_key, VistaDBBoolean is_unique)
        {
            IndexSchemaEntry indexSchemaEntry = (IndexSchemaEntry)source;
            index_catalog.Value = indexSchemaEntry.Catalog;
            index_schema.Value = indexSchemaEntry.Owner;
            index_name.Value = indexSchemaEntry.Name;
            table_name.Value = indexSchemaEntry.Table;
            is_primary_key.Value = indexSchemaEntry.IsPrimary;
            is_unique.Value = indexSchemaEntry.IsUnique;
        }

        [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillIndexColumnSchema")]
        public static IEnumerable IndexColumnSchema()
        {
            List<IndexColumnSchemaEntry> columnSchemaEntryList = new List<IndexColumnSchemaEntry>();
            Database currentDatabase = GetCurrentDatabase();
            string databaseName = GetDatabaseName(currentDatabase);
            foreach (string key in (IEnumerable<string>)currentDatabase.GetTableIdMap().Keys)
            {
                IVistaDBTableSchema tableSchema = currentDatabase.GetTableSchema(key, false);
                foreach (IVistaDBIndexInformation index in (IEnumerable<IVistaDBIndexInformation>)tableSchema.Indexes.Values)
                {
                    if (!index.FKConstraint)
                    {
                        foreach (IVistaDBKeyColumn column in index.KeyStructure)
                            columnSchemaEntryList.Add(new IndexColumnSchemaEntry(databaseName, tableSchema, index, column));
                    }
                }
            }
            return columnSchemaEntryList;
        }

        public static void FillIndexColumnSchema(object source, VistaDBString index_catalog, VistaDBString index_schema, VistaDBString index_name, VistaDBString table_name, VistaDBString column_name, VistaDBInt32 key_ordinal)
        {
            IndexColumnSchemaEntry columnSchemaEntry = (IndexColumnSchemaEntry)source;
            index_catalog.Value = columnSchemaEntry.Catalog;
            index_schema.Value = columnSchemaEntry.Owner;
            index_name.Value = columnSchemaEntry.Index;
            table_name.Value = columnSchemaEntry.Table;
            column_name.Value = columnSchemaEntry.Name;
            key_ordinal.Value = columnSchemaEntry.Ordinal;
        }

        [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillRoutineSchema")]
        public static IEnumerable RoutineSchema()
        {
            List<RoutineSchemaEntry> routineSchemaEntryList = new List<RoutineSchemaEntry>();
            Database currentDatabase = GetCurrentDatabase();
            string databaseName = GetDatabaseName(currentDatabase);
            foreach (IStoredProcedureInformation sp in (IEnumerable<IStoredProcedureInformation>)currentDatabase.LoadSqlStoredProcedures().Values)
            {
                List<SQLParser.VariableDeclaration> variables;
                Statement procedure = ParseProcedure(currentDatabase, sp.Statement, out variables);
                routineSchemaEntryList.Add(new RoutineSchemaEntry(databaseName, sp, procedure, variables));
            }
            return routineSchemaEntryList;
        }

        public static void FillRoutineSchema(object source, VistaDBString specific_catalog, VistaDBString specific_schema, VistaDBString specific_name, VistaDBString routine_catalog, VistaDBString routine_schema, VistaDBString routine_name, VistaDBString routine_type, VistaDBString module_catalog, VistaDBString module_schema, VistaDBString module_name, VistaDBString udt_catalog, VistaDBString udt_schema, VistaDBString udt_name, VistaDBString data_type, VistaDBInt32 character_maximum_length, VistaDBInt32 character_octet_length, VistaDBString collation_catalog, VistaDBString collation_schema, VistaDBString collation_name, VistaDBString character_set_catalog, VistaDBString character_set_schema, VistaDBString character_set_name, VistaDBByte numeric_precision, VistaDBInt16 numeric_precision_radix, VistaDBInt16 numeric_scale, VistaDBInt16 datetime_precision, VistaDBString interval_type, VistaDBInt16 interval_precision, VistaDBString type_udt_catalog, VistaDBString type_udt_schema, VistaDBString type_udt_name, VistaDBString scope_catalog, VistaDBString scope_schema, VistaDBString scope_name, VistaDBInt64 maximum_cardinality, VistaDBString dtd_identifier, VistaDBString routine_body, VistaDBString routine_definition, VistaDBString external_name, VistaDBString external_language, VistaDBString parameter_style, VistaDBString is_deterministic, VistaDBString sql_data_access, VistaDBString is_null_call, VistaDBString sql_path, VistaDBString schema_level_routine, VistaDBInt16 max_dynamic_result_set, VistaDBString is_user_defined_cast, VistaDBString is_implicitly_invocable, VistaDBDateTime created, VistaDBDateTime last_altered)
        {
            RoutineSchemaEntry routineSchemaEntry = (RoutineSchemaEntry)source;
            specific_catalog.Value = routineSchemaEntry.Catalog;
            specific_name.Value = routineSchemaEntry.Name;
            specific_schema.Value = routineSchemaEntry.Owner;
            routine_catalog.Value = routineSchemaEntry.Catalog;
            routine_name.Value = routineSchemaEntry.Name;
            routine_schema.Value = routineSchemaEntry.Owner;
            routine_type.Value = routineSchemaEntry.IsFunction ? "FUNCTION" : (object)"PROCEDURE";
            module_catalog.Value = null;
            module_name.Value = null;
            module_schema.Value = null;
            udt_catalog.Value = null;
            udt_name.Value = null;
            udt_schema.Value = null;
            if (routineSchemaEntry.IsFunction && routineSchemaEntry.IsTable)
                data_type.Value = "table";
            else
                data_type.Value = routineSchemaEntry.DataType.ToString().ToLower();
            character_maximum_length.Value = null;
            character_octet_length.Value = null;
            numeric_precision.Value = null;
            numeric_precision_radix.Value = null;
            numeric_scale.Value = null;
            datetime_precision.Value = null;
            if (routineSchemaEntry.DateTimeSub != 0)
                datetime_precision.Value = routineSchemaEntry.DateTimeSub;
            else if (routineSchemaEntry.Radix != 0)
            {
                numeric_precision.Value = (byte)routineSchemaEntry.Precision;
                numeric_precision_radix.Value = routineSchemaEntry.Radix;
                numeric_scale.Value = routineSchemaEntry.Scale;
            }
            else if (routineSchemaEntry.MaxLength != 0)
            {
                character_maximum_length.Value = routineSchemaEntry.Precision;
                character_octet_length.Value = routineSchemaEntry.OctetLength;
            }
            character_set_catalog.Value = null;
            character_set_schema.Value = null;
            character_set_name.Value = null;
            collation_catalog.Value = null;
            collation_schema.Value = null;
            collation_name.Value = null;
            interval_type.Value = null;
            interval_precision.Value = null;
            type_udt_catalog.Value = null;
            type_udt_schema.Value = null;
            type_udt_name.Value = null;
            scope_catalog.Value = null;
            scope_schema.Value = null;
            scope_name.Value = null;
            maximum_cardinality.Value = null;
            dtd_identifier.Value = null;
            routine_body.Value = routineSchemaEntry.Body == null ? "EXTERNAL" : (object)routineSchemaEntry.Body;
            routine_definition.Value = null;
            external_name.Value = null;
            external_language.Value = null;
            parameter_style.Value = null;
            is_deterministic.Value = !routineSchemaEntry.IsFunction ? "NO" : (object)"YES";
            sql_data_access.Value = !routineSchemaEntry.IsFunction ? "MODIFIED" : (object)"READS";
            is_null_call.Value = "YES";
            sql_path.Value = null;
            schema_level_routine.Value = "YES";
            max_dynamic_result_set.Value = (short)(routineSchemaEntry.IsFunction ? 0 : (short)routineSchemaEntry.MaxResults);
            is_user_defined_cast.Value = "NO";
            is_implicitly_invocable.Value = "NO";
            created.Value = routineSchemaEntry.Created;
            last_altered.Value = routineSchemaEntry.LastAltered;
        }

        [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillParameterSchema")]
        public static IEnumerable ParameterSchema()
        {
            List<ParameterSchemaEntry> parameterSchemaEntryList = new List<ParameterSchemaEntry>();
            string databaseName = GetDatabaseName(GetCurrentDatabase());
            foreach (DataRow row in (InternalDataCollectionBase)new VistaDBConnection(VistaDBContext.DDAChannel.CurrentDatabase).GetSchema("PROCEDUREPARAMETERS").Rows)
                parameterSchemaEntryList.Add(new ParameterSchemaEntry(databaseName, row));
            return parameterSchemaEntryList;
        }

        public static void FillParameterSchema(object source, VistaDBString specific_catalog, VistaDBString specific_schema, VistaDBString specific_name, VistaDBInt32 ordinal_position, VistaDBString parameter_mode, VistaDBString is_result, VistaDBString as_locator, VistaDBString parameter_name, VistaDBString data_type, VistaDBInt32 character_maximum_length, VistaDBInt32 character_octet_length, VistaDBString collation_catalog, VistaDBString collation_schema, VistaDBString collation_name, VistaDBString character_set_catalog, VistaDBString character_set_schema, VistaDBString character_set_name, VistaDBByte numeric_precision, VistaDBInt16 numeric_precision_radix, VistaDBInt16 numeric_scale, VistaDBInt16 datetime_precision, VistaDBString interval_type, VistaDBInt16 interval_precision, VistaDBString user_defined_type_catalog, VistaDBString user_defined_type_schema, VistaDBString user_defined_type_name, VistaDBString scope_catalog, VistaDBString scope_schema, VistaDBString scope_name)
        {
            ParameterSchemaEntry parameterSchemaEntry = source as ParameterSchemaEntry;
            specific_catalog.Value = parameterSchemaEntry.SpecificCatalog;
            specific_schema.Value = parameterSchemaEntry.SpecificSchema;
            specific_name.Value = parameterSchemaEntry.SpecificName;
            if (parameterSchemaEntry.ParameterMode == 1)
                parameter_mode.Value = "OUT";
            else
                parameter_mode.Value = "IN";
            ordinal_position.Value = parameterSchemaEntry.OrdinalPosition + 1;
            is_result.Value = parameterSchemaEntry.IsResult ? "YES" : (object)"NO";
            as_locator.Value = parameterSchemaEntry.AsLocator ? "YES" : (object)"NO";
            parameter_name.Value = parameterSchemaEntry.ParameterName;
            data_type.Value = parameterSchemaEntry.DataType.ToLowerInvariant();
            character_maximum_length.Value = parameterSchemaEntry.CharacterMaxLength;
            character_octet_length.Value = parameterSchemaEntry.CharacterOctetLength;
            collation_catalog.Value = parameterSchemaEntry.CollationCatalog;
            collation_schema.Value = parameterSchemaEntry.CollationSchema;
            collation_name.Value = parameterSchemaEntry.CollationName;
            character_set_catalog.Value = parameterSchemaEntry.CharacterSetCatalog;
            character_set_schema.Value = parameterSchemaEntry.CharacterSetSchema;
            character_set_name.Value = parameterSchemaEntry.CharacterSetName;
            numeric_precision.Value = parameterSchemaEntry.NumericPrecision;
            numeric_precision_radix.Value = parameterSchemaEntry.NumericPrecisionRadix;
            numeric_scale.Value = parameterSchemaEntry.NumericScale;
            datetime_precision.Value = parameterSchemaEntry.DateTimePrecision;
            interval_type.Value = parameterSchemaEntry.IntervalType;
            interval_precision.Value = parameterSchemaEntry.IntervalPrecision;
            user_defined_type_catalog.Value = parameterSchemaEntry.UserDefinedTypeCatalog;
            user_defined_type_schema.Value = parameterSchemaEntry.UserDefinedTypeSchema;
            user_defined_type_name.Value = parameterSchemaEntry.UserDefinedTypeName;
            scope_catalog.Value = parameterSchemaEntry.ScopeCatalog;
            scope_schema.Value = parameterSchemaEntry.ScopeSchema;
            scope_name.Value = parameterSchemaEntry.ScopeName;
        }

        [VistaDBClrProcedure(FillRow = "VistaDB.Engine.Functions.SystemFunctions.FillRoutineColumnSchema")]
        public static IEnumerable RoutineColumnSchema()
        {
            return new List<object>();
        }

        public static void FillRoutineColumnSchema(object source, VistaDBString table_catalog, VistaDBString table_schema, VistaDBString table_name, VistaDBString column_name, VistaDBInt32 ordinal_position, VistaDBString column_default, VistaDBString is_nullable, VistaDBString data_type, VistaDBInt32 character_maximum_length, VistaDBInt32 character_octet_length, VistaDBByte numeric_precision, VistaDBInt16 numeric_precision_radix, VistaDBInt16 numeric_scale, VistaDBInt16 datetime_precision, VistaDBString character_set_catalog, VistaDBString character_set_schema, VistaDBString character_set_name, VistaDBString collation_catalog, VistaDBString collation_schema, VistaDBString collation_name, VistaDBString domain_catalog, VistaDBString domain_schema, VistaDBString domain_name)
        {
        }

        [VistaDBClrProcedure]
        public static VistaDBInt32 ObjectId(VistaDBString object_name)
        {
            return ObjectId2(object_name, new VistaDBString());
        }

        public static VistaDBInt32 ObjectId2(VistaDBString object_name, VistaDBString object_type)
        {
            string[] strArray = ((string)object_name.Value).Split(new char[3] { '.', '[', ']' }, StringSplitOptions.RemoveEmptyEntries);
            string str = strArray[strArray.Length - 1];
            int hashCode = str.GetHashCode();
            if (!_objIds.ContainsKey(hashCode))
                _objIds.Add(hashCode, str);
            return new VistaDBInt32(hashCode);
        }

        public static VistaDBString DatabaseName()
        {
            Database currentDatabase = GetCurrentDatabase();
            if (currentDatabase != null)
                return new VistaDBString(GetDatabaseName(currentDatabase));
            using (VistaDBConnection currentConnection = GetCurrentConnection())
            {
                if (currentConnection != null)
                    return new VistaDBString(GetDatabaseName(currentConnection.Database));
            }
            return new VistaDBString();
        }

        [VistaDBClrProcedure]
        public static VistaDBInt32 ColumnProperty(VistaDBInt32 id, VistaDBString column, VistaDBString property)
        {
            if (!_objIds.ContainsKey((int)id.Value))
                return new VistaDBInt32();
            string objId = _objIds[(int)id.Value];
            string index = (string)column.Value;
            string str = (string)property.Value;
            Database currentDatabase = GetCurrentDatabase();
            IVistaDBTableSchema vistaDbTableSchema = null;
            if (currentDatabase != null)
            {
                vistaDbTableSchema = currentDatabase.GetTableSchema(objId, true);
            }
            else
            {
                using (VistaDBConnection currentConnection = GetCurrentConnection())
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
            switch (((string)propertyName.Value).ToLowerInvariant())
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
            if ((long)id.Value == 0L)
                return new VistaDBString("sa");
            return SchemaName();
        }

        [VistaDBClrProcedure]
        public static VistaDBString SchemaName()
        {
            return new VistaDBString("dbo");
        }

        public static void RegisterIntoHosting(ClrHosting hosting, IList<string> systemProcedures)
        {
            Assembly assembly = GetAssembly();
            if (assembly == null)
                return;
            try
            {
                ClrHosting.ClrProcedure method1 = GetMethod("TableSchema", assembly);
                hosting.AddProcedure("VistaDBTableSchema", method1);
                ClrHosting.ClrProcedure method2 = GetMethod("EF_Tables_Query", assembly);
                hosting.AddProcedure("VistaDBEFTables", method2);
                ClrHosting.ClrProcedure method3 = GetMethod("EF_TableColumns_Query", assembly);
                hosting.AddProcedure("VistaDBEFTableColumns", method3);
                ClrHosting.ClrProcedure method4 = GetMethod("EF_Views_Query", assembly);
                hosting.AddProcedure("VistaDBEFViews", method4);
                ClrHosting.ClrProcedure method5 = GetMethod("EF_ViewColumns_Query", assembly);
                hosting.AddProcedure("VistaDBEFViewColumns", method5);
                ClrHosting.ClrProcedure method6 = GetMethod("EF_Constraints_Query", assembly);
                hosting.AddProcedure("VistaDBEFConstraints", method6);
                ClrHosting.ClrProcedure method7 = GetMethod("EF_ConstraintColumns_Query", assembly);
                hosting.AddProcedure("VistaDBEFConstraintColumns", method7);
                ClrHosting.ClrProcedure method8 = GetMethod("EF_ForeignKeyConstraints_Query", assembly);
                hosting.AddProcedure("VistaDBEFForeignKeyConstraints", method8);
                ClrHosting.ClrProcedure method9 = GetMethod("EF_ForeignKeys_Query", assembly);
                hosting.AddProcedure("VistaDBEFForeignKeys", method9);
                ClrHosting.ClrProcedure method10 = GetMethod("ColumnSchema", assembly);
                hosting.AddProcedure("VistaDBColumnSchema", method10);
                ClrHosting.ClrProcedure method11 = GetMethod("ViewSchema", assembly);
                hosting.AddProcedure("VistaDBViewSchema", method11);
                ClrHosting.ClrProcedure method12 = GetMethod("TableConstraintSchema", assembly);
                hosting.AddProcedure("VistaDBTableConstraintSchema", method12);
                ClrHosting.ClrProcedure method13 = GetMethod("CheckConstraintSchema", assembly);
                hosting.AddProcedure("VistaDBCheckConstraintSchema", method13);
                ClrHosting.ClrProcedure method14 = GetMethod("ReferentialConstraintSchema", assembly);
                hosting.AddProcedure("VistaDBReferentialConstraintSchema", method14);
                ClrHosting.ClrProcedure method15 = GetMethod("ForeignKeySchema", assembly);
                hosting.AddProcedure("VistaDBForeignKeySchema", method15);
                ClrHosting.ClrProcedure method16 = GetMethod("ForeignKeyColumnSchema", assembly);
                hosting.AddProcedure("VistaDBForeignKeyColumnSchema", method16);
                ClrHosting.ClrProcedure method17 = GetMethod("IndexSchema", assembly);
                hosting.AddProcedure("VistaDBIndexSchema", method17);
                ClrHosting.ClrProcedure method18 = GetMethod("IndexColumnSchema", assembly);
                hosting.AddProcedure("VistaDBIndexColumnSchema", method18);
                ClrHosting.ClrProcedure method19 = GetMethod("KeyColumnUsageSchema", assembly);
                hosting.AddProcedure("VistaDBKeyColumnUsageSchema", method19);
                ClrHosting.ClrProcedure method20 = GetMethod("RoutineSchema", assembly);
                hosting.AddProcedure("VistaDBRoutineSchema", method20);
                ClrHosting.ClrProcedure method21 = GetMethod("ParameterSchema", assembly);
                hosting.AddProcedure("VistaDBParameterSchema", method21);
                ClrHosting.ClrProcedure method22 = GetMethod("ObjectId", assembly);
                hosting.AddProcedure("Object_Id", method22);
                systemProcedures.Add("Object_Id");
                ClrHosting.ClrProcedure method23 = GetMethod("ColumnProperty", assembly);
                hosting.AddProcedure("ColumnProperty", method23);
                systemProcedures.Add("ColumnProperty");
                ClrHosting.ClrProcedure method24 = GetMethod("ServerProperty", assembly);
                hosting.AddProcedure("ServerProperty", method24);
                systemProcedures.Add("ServerProperty");
                ClrHosting.ClrProcedure method25 = GetMethod("UserId", assembly);
                hosting.AddProcedure("SUser_SID", method25);
                systemProcedures.Add("SUser_SID");
                ClrHosting.ClrProcedure method26 = GetMethod("UserName", assembly);
                hosting.AddProcedure("SUser_SNAME", method26);
                systemProcedures.Add("SUser_SNAME");
                ClrHosting.ClrProcedure method27 = GetMethod("SchemaName", assembly);
                hosting.AddProcedure("Schema_Name", method27);
                systemProcedures.Add("Schema_Name");
                hosting.AddProcedure("User_Name", method27);
                systemProcedures.Add("User_Name");
                ClrHosting.ClrProcedure method28 = GetMethod("DatabaseName", assembly);
                hosting.AddProcedure("DB_Name", method28);
                systemProcedures.Add("DB_Name");
            }
            catch (Exception)
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
                _db = db_name;
                _name = schema.Name;
                _owner = "dbo";
                _type = "BASE TABLE";
            }

            public TableSchemaEntry(string db_name, Database.ViewList.View schema)
            {
                _db = db_name;
                _name = schema.Name;
                _owner = "dbo";
                _type = "VIEW";
            }

            public string Catalog
            {
                get
                {
                    return _db;
                }
            }

            public string Name
            {
                get
                {
                    return _name;
                }
            }

            public string Owner
            {
                get
                {
                    return _owner;
                }
            }

            public string TableType
            {
                get
                {
                    return _type;
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
                _db = db_name;
                _owner = "dbo";
                _table = table.Name;
                _name = schema.Name;
                _isStoreGenerated = schema.Type == VistaDBType.Timestamp;
                _index = schema.RowIndex;
                if (defaults != null)
                    _default = defaults.Expression;
                _nullable = schema.AllowNull;
                _type = schema.Type;
                _maxLength = schema.MaxLength;
                _identity = identity != null;
                GetTypeInfo(_table, _name, _type, ref _maxLength, ref _octetLength, ref _precision, ref _scale, ref _radix, ref _dateTime);
            }

            public ColumnSchemaEntry(string catalog, DataRow column)
            {
                _db = catalog;
                _owner = "dbo";
                _table = column["VIEW_NAME"] as string;
                _name = column["COLUMN_NAME"] as string;
                _index = (int)column["ORDINAL_POSITION"];
                if ((bool)column["COLUMN_HASDEFAULT"])
                    _default = column["COLUMN_DEFAULT"] as string;
                _nullable = (bool)column["IS_NULLABLE"];
                _type = (VistaDBType)Enum.Parse(typeof(VistaDBType), column["DATA_TYPE"] as string);
                _isStoreGenerated = _type == VistaDBType.Timestamp;
                _maxLength = (int)column["CHARACTER_MAXIMUM_LENGTH"];
                GetTypeInfo(_table, _name, _type, ref _maxLength, ref _octetLength, ref _precision, ref _scale, ref _radix, ref _dateTime);
            }

            public string Catalog
            {
                get
                {
                    return _db;
                }
            }

            public string Owner
            {
                get
                {
                    return _owner;
                }
            }

            public string Table
            {
                get
                {
                    return _table;
                }
            }

            public string Name
            {
                get
                {
                    return _name;
                }
            }

            public int Index
            {
                get
                {
                    return _index;
                }
            }

            public string Default
            {
                get
                {
                    return _default;
                }
            }

            public bool Nullable
            {
                get
                {
                    return _nullable;
                }
            }

            public VistaDBType DataType
            {
                get
                {
                    return _type;
                }
            }

            public int MaxLength
            {
                get
                {
                    return _maxLength;
                }
            }

            public int Precision
            {
                get
                {
                    return _precision;
                }
            }

            public short Scale
            {
                get
                {
                    return _scale;
                }
            }

            public int OctetLength
            {
                get
                {
                    return _octetLength;
                }
            }

            public short Radix
            {
                get
                {
                    return _radix;
                }
            }

            public short DateTimeSub
            {
                get
                {
                    return _dateTime;
                }
            }

            public bool IsIdentity
            {
                get
                {
                    return _identity;
                }
            }

            public bool IsStoreGenerated
            {
                get
                {
                    return _isStoreGenerated;
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
                _db = catalog;
                _name = schema.Name;
                _def = schema.Expression;
                _owner = "dbo";
            }

            public string Catalog
            {
                get
                {
                    return _db;
                }
            }

            public string Name
            {
                get
                {
                    return _name;
                }
            }

            public string Expression
            {
                get
                {
                    return _def;
                }
            }

            public string Owner
            {
                get
                {
                    return _owner;
                }
            }
        }

        private class ViewColumnDataRow
        {
            public ViewColumnDataRow(string catalog, DataRow row)
            {
                ViewCatalog = row["VIEW_CATALOG"] as string ?? catalog;
                ViewSchema = row["VIEW_SCHEMA"] as string ?? "dbo";
                ViewName = row["VIEW_NAME"] as string;
                TableCatalog = row["TABLE_CATALOG"] as string ?? catalog;
                TableSchema = row["TABLE_SCHEMA"] as string ?? "dbo";
                TableName = row["TABLE_NAME"] as string;
                Name = row["COLUMN_NAME"] as string;
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
                _owner = "dbo";
                _db = db_name;
                _name = index.Name;
                _table = tableName;
                if (index.Primary)
                    _type = "PRIMARY KEY";
                else if (index.Unique)
                {
                    _type = "UNIQUE";
                }
                else
                {
                    if (!index.FKConstraint)
                        throw new Exception("Only call this constructor on primary or unique indexes!");
                    _type = "FOREIGN KEY";
                }
                _expression = index.KeyExpression;
            }

            public RelationshipSchemaEntry(string db_name, string tableName, IVistaDBConstraintInformation constraint)
            {
                _owner = "dbo";
                _db = db_name;
                _name = constraint.Name;
                _table = tableName;
                _expression = constraint.Expression;
                _type = "CHECK";
            }

            public RelationshipSchemaEntry(string db_name, IVistaDBRelationshipInformation relation, IVistaDBIndexInformation foreign_index)
            {
                _owner = "dbo";
                _db = db_name;
                _name = relation.Name;
                _table = relation.ForeignTable;
                _expression = foreign_index.Name;
                _type = "FOREIGN KEY";
                switch (relation.UpdateIntegrity)
                {
                    case VistaDBReferentialIntegrity.Cascade:
                        _update = "CASCADE";
                        break;
                    case VistaDBReferentialIntegrity.SetNull:
                        _update = "SET NULL";
                        break;
                    case VistaDBReferentialIntegrity.SetDefault:
                        _update = "SET DEFAULT";
                        break;
                    default:
                        _update = "NO ACTION";
                        break;
                }
                switch (relation.DeleteIntegrity)
                {
                    case VistaDBReferentialIntegrity.Cascade:
                        _delete = "CASCADE";
                        break;
                    case VistaDBReferentialIntegrity.SetNull:
                        _delete = "SET NULL";
                        break;
                    case VistaDBReferentialIntegrity.SetDefault:
                        _delete = "SET DEFAULT";
                        break;
                    default:
                        _delete = "NO ACTION";
                        break;
                }
            }

            public RelationshipSchemaEntry(string db_name, IVistaDBTableSchema table, IVistaDBIndexInformation index, IVistaDBTableSchema primaryTable, IVistaDBKeyColumn key)
            {
                _db = db_name;
                _table = table.Name;
                _owner = "dbo";
                _name = index.Name;
                _expression = primaryTable[key.RowIndex].Name;
                _ordinal = key.RowIndex + 1;
            }

            public string Catalog
            {
                get
                {
                    return _db;
                }
            }

            public string Name
            {
                get
                {
                    return _name;
                }
            }

            public string Owner
            {
                get
                {
                    return _owner;
                }
            }

            public string Table
            {
                get
                {
                    return _table;
                }
            }

            public string Expression
            {
                get
                {
                    return _expression;
                }
            }

            public string RelationType
            {
                get
                {
                    return _type;
                }
            }

            public string UpdateRule
            {
                get
                {
                    return _update;
                }
            }

            public string DeleteRule
            {
                get
                {
                    return _delete;
                }
            }

            public int ColumnOrdinal
            {
                get
                {
                    return _ordinal;
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
                _db = db_name;
                _name = index.Name;
                _table = schema.Name;
                _primary = relation.PrimaryTable;
                _owner = "dbo";
                _delete = (int)relation.DeleteIntegrity;
                _update = (int)relation.UpdateIntegrity;
            }

            public string Catalog
            {
                get
                {
                    return _db;
                }
            }

            public string Name
            {
                get
                {
                    return _name;
                }
            }

            public string Owner
            {
                get
                {
                    return _owner;
                }
            }

            public string Table
            {
                get
                {
                    return _table;
                }
            }

            public string TargetTable
            {
                get
                {
                    return _primary;
                }
            }

            public int Update
            {
                get
                {
                    return _update;
                }
            }

            public int Delete
            {
                get
                {
                    return _delete;
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
                _db = db_name;
                _name = index.Name;
                _table = schema.Name;
                _key = schema[column.RowIndex].Name;
                _column = targetSchema[targetKey.KeyStructure[0].RowIndex].Name;
                _ordinal = targetKey.KeyStructure[0].RowIndex;
                _owner = "dbo";
            }

            public string Catalog
            {
                get
                {
                    return _db;
                }
            }

            public string Name
            {
                get
                {
                    return _name;
                }
            }

            public string Owner
            {
                get
                {
                    return _owner;
                }
            }

            public string Table
            {
                get
                {
                    return _table;
                }
            }

            public string Key
            {
                get
                {
                    return _key;
                }
            }

            public string Column
            {
                get
                {
                    return _column;
                }
            }

            public int Ordinal
            {
                get
                {
                    return _ordinal;
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
                _db = db_name;
                _name = index.Name;
                _table = schema.Name;
                _owner = "dbo";
                _primary = index.Primary;
                _unique = index.Unique;
            }

            public string Catalog
            {
                get
                {
                    return _db;
                }
            }

            public string Name
            {
                get
                {
                    return _name;
                }
            }

            public string Owner
            {
                get
                {
                    return _owner;
                }
            }

            public string Table
            {
                get
                {
                    return _table;
                }
            }

            public bool IsPrimary
            {
                get
                {
                    return _primary;
                }
            }

            public bool IsUnique
            {
                get
                {
                    return _unique;
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
                _db = db_name;
                _index = index.Name;
                _table = schema.Name;
                _owner = "dbo";
                _name = schema[column.RowIndex].Name;
                _ordinal = column.RowIndex;
            }

            public string Catalog
            {
                get
                {
                    return _db;
                }
            }

            public string Name
            {
                get
                {
                    return _name;
                }
            }

            public string Owner
            {
                get
                {
                    return _owner;
                }
            }

            public string Table
            {
                get
                {
                    return _table;
                }
            }

            public string Index
            {
                get
                {
                    return _index;
                }
            }

            public int Ordinal
            {
                get
                {
                    return _ordinal;
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
            private readonly VistaDBType _type;
            private int _maxLength;
            private int _precision;
            private short _scale;
            private int _octetLength;
            private short _radix;
            private short _dateTime;
            private string _body;
            private int _results;
            private DateTime _created;
            private readonly DateTime _altered;

            public RoutineSchemaEntry(string db, IStoredProcedureInformation sp, Statement statement, List<SQLParser.VariableDeclaration> variables)
            {
                _db = db;
                _owner = "dbo";
                _func = false;
                _table = false;
                _name = sp.Name;
                _body = sp.Statement;
                GetTypeInfo(_name, "Return", _type, ref _maxLength, ref _octetLength, ref _precision, ref _scale, ref _radix, ref _dateTime);
            }

            public string Catalog
            {
                get
                {
                    return _db;
                }
            }

            public string Owner
            {
                get
                {
                    return _owner;
                }
            }

            public string Name
            {
                get
                {
                    return _name;
                }
            }

            public bool IsFunction
            {
                get
                {
                    return _func;
                }
            }

            public bool IsTable
            {
                get
                {
                    return _table;
                }
            }

            public VistaDBType DataType
            {
                get
                {
                    return _type;
                }
            }

            public int MaxLength
            {
                get
                {
                    return _maxLength;
                }
            }

            public int Precision
            {
                get
                {
                    return _precision;
                }
            }

            public short Scale
            {
                get
                {
                    return _scale;
                }
            }

            public int OctetLength
            {
                get
                {
                    return _octetLength;
                }
            }

            public short Radix
            {
                get
                {
                    return _radix;
                }
            }

            public short DateTimeSub
            {
                get
                {
                    return _dateTime;
                }
            }

            public string Body
            {
                get
                {
                    return _body;
                }
            }

            public int MaxResults
            {
                get
                {
                    return _results;
                }
            }

            public DateTime Created
            {
                get
                {
                    return _created;
                }
            }

            public DateTime LastAltered
            {
                get
                {
                    return _altered;
                }
            }
        }

        private class ParameterSchemaEntry
        {
            public ParameterSchemaEntry(string catalog, DataRow row)
            {
                SpecificCatalog = row["SPECIFIC_CATALOG"] as string ?? catalog;
                SpecificSchema = row["SPECIFIC_SCHEMA"] as string ?? "dbo";
                SpecificName = row["PROCEDURE_NAME"] as string;
                OrdinalPosition = Convert.ToInt16(row["ORDINAL_POSITION"]);
                ParameterMode = Convert.ToInt32(row["PARAMETER_DIRECTION"]);
                IsResult = false;
                AsLocator = false;
                ParameterName = row["PARAMETER_NAME"] as string;
                DataType = row["PARAMETER_DATA_TYPE"] as string;
                VistaDBType type = (VistaDBType)Enum.Parse(typeof(VistaDBType), DataType);
                CharacterMaxLength = new int?();
                CharacterOctetLength = new int?();
                CharacterSetCatalog = null;
                CharacterSetSchema = null;
                CharacterSetName = null;
                CollationCatalog = null;
                CollationSchema = null;
                CollationName = null;
                NumericPrecision = new byte?();
                NumericPrecisionRadix = new short?();
                NumericScale = new short?();
                DateTimePrecision = new short?();
                IntervalType = null;
                IntervalPrecision = new short?();
                UserDefinedTypeCatalog = null;
                UserDefinedTypeSchema = null;
                UserDefinedTypeName = null;
                ScopeCatalog = null;
                ScopeSchema = null;
                SpecificCatalog = null;
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
                GetTypeInfo(SpecificName, ParameterName, type, ref maxLength, ref octetLength, ref precision, ref scale, ref radix, ref dateTime);
                if (maxLength != 0)
                    CharacterMaxLength = new int?(maxLength);
                if (octetLength != -1)
                    CharacterOctetLength = new int?(octetLength);
                if (precision != 0 && precision < byte.MaxValue)
                    NumericPrecision = new byte?((byte)precision);
                if (scale != -1)
                    NumericScale = new short?(scale);
                if (radix != 0)
                    NumericPrecisionRadix = new short?(radix);
                if (dateTime == 0)
                    return;
                DateTimePrecision = new short?(dateTime);
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
