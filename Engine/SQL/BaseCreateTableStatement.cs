using System;
using System.Collections.Generic;
using System.Text;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
    internal abstract class BaseCreateTableStatement : BaseCreateStatement
    {
        protected int nameIndex = 1;
        protected PrimaryKey primaryKey;
        protected string tableDescription;
        protected string tableName;
        protected ColumnDescrList columns;
        protected UniqueColumnList uniqueColumns;
        protected ForeignKeyList foreignKeys;
        protected CheckList checks;

        protected BaseCreateTableStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
          : base(connection, parent, parser, id)
        {
        }

        protected override void DoBeforeParse()
        {
            base.DoBeforeParse();
            columns = new ColumnDescrList(this);
            uniqueColumns = new UniqueColumnList(this);
            foreignKeys = new ForeignKeyList(this);
            checks = new CheckList(this);
        }

        protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
        {
            if (id < 0L)
                return;
            parser.SkipToken(true);
            tableName = parser.GetTableName(this);
            parser.SkipToken(true);
        }

        protected bool ParseColumns(SQLParser parser)
        {
            SQLParser.TokenValueClass tokenValue = parser.TokenValue;
            do
            {
                parser.SkipToken(true);
                if (tokenValue.TokenType == TokenType.Unknown && (parser.IsToken("CONSTRAINT") || parser.IsToken("CHECK") || (parser.IsToken("PRIMARY") || parser.IsToken("UNIQUE")) || parser.IsToken("FOREIGN")))
                    return ParseTableConstraints(parser);
                int rowNo = tokenValue.RowNo;
                int colNo = tokenValue.ColNo;
                string columnName;
                VistaDBType dataType;
                int width;
                int codePage;
                bool allowNull;
                bool readOnly;
                bool encrypted;
                bool packed;
                string defaultValue;
                bool setIdentity;
                string identitySeed;
                string identityStep;
                string caption;
                string description;
                ParseColumnAttributes(out columnName, out dataType, out width, out codePage, out allowNull, out readOnly, out encrypted, out packed, out defaultValue, out setIdentity, out identitySeed, out identityStep, out caption, out description, parser);
                if (dataType == VistaDBType.Unknown)
                    throw new VistaDBSQLException(508, tokenValue.Token, tokenValue.RowNo, tokenValue.ColNo);
                bool columnConstraints = ParseColumnConstraints(columnName, parser);
                if (parser.IsToken("ROWGUIDCOL"))
                {
                    if (dataType != VistaDBType.UniqueIdentifier)
                        throw new VistaDBSQLException(655, tokenValue.Token, tokenValue.RowNo, tokenValue.ColNo);
                    defaultValue = "NEWID" + "()";
                    readOnly = true;
                    parser.SkipToken(false);
                }
                columns.Add(new ColumnDescr(this, rowNo, colNo, columnName, dataType, width, codePage, allowNull, readOnly, encrypted, packed, defaultValue, setIdentity, identitySeed, identityStep, caption, description));
                if (!columnConstraints)
                    return false;
            }
            while (parser.IsToken(","));
            return true;
        }

        protected bool ParseColumnAttributes(out string columnName, out VistaDBType dataType, out int width, out int codePage, out bool allowNull, out bool readOnly, out bool encrypted, out bool packed, out string defaultValue, out bool setIdentity, out string identitySeed, out string identityStep, out string caption, out string description, SQLParser parser)
        {
            SQLParser.TokenValueClass tokenValue = parser.TokenValue;
            int lineNo = 0;
            int symbolNo = 0;
            columnName = null;
            dataType = VistaDBType.Unknown;
            width = 30;
            codePage = 0;
            allowNull = true;
            readOnly = false;
            encrypted = false;
            packed = false;
            defaultValue = null;
            setIdentity = false;
            identitySeed = null;
            identityStep = null;
            caption = null;
            description = null;
            columnName = tokenValue.Token;
            parser.SkipToken(true);
            dataType = parser.ReadDataType(out width);
            if (parser.EndOfText)
                return false;
            if (parser.IsToken("CODE"))
            {
                parser.SkipToken(true);
                parser.ExpectedExpression("PAGE");
                parser.SkipToken(true);
                codePage = StrTokenToInt(parser);
                if (!parser.SkipToken(false))
                    return false;
            }
            do
            {
                bool flag;
                if (parser.IsToken("NOT"))
                {
                    flag = true;
                    lineNo = tokenValue.RowNo;
                    symbolNo = tokenValue.ColNo;
                    parser.SkipToken(true);
                }
                else
                    flag = false;
                if (parser.IsToken("NULL"))
                    allowNull = !flag;
                else if (parser.IsToken("READ"))
                {
                    parser.SkipToken(true);
                    parser.ExpectedExpression("ONLY");
                    readOnly = !flag;
                }
                else if (parser.IsToken("ENCRYPTED"))
                    encrypted = !flag;
                else if (parser.IsToken("PACKED"))
                {
                    packed = !flag;
                }
                else
                {
                    if (flag)
                        throw new VistaDBSQLException(507, "NULL, ENCRYPTED, PACKED", lineNo, symbolNo);
                    if (parser.IsToken("DEFAULT"))
                    {
                        parser.SkipToken(true);
                        defaultValue = tokenValue.Token;
                    }
                    else if (parser.IsToken("IDENTITY"))
                    {
                        parser.SkipToken(true);
                        parser.ExpectedExpression("(");
                        parser.SkipToken(true);
                        identitySeed = tokenValue.Token;
                        if (parser.IsToken("-"))
                        {
                            parser.SkipToken(true);
                            identitySeed += tokenValue.Token;
                        }
                        parser.SkipToken(true);
                        parser.ExpectedExpression(",");
                        parser.SkipToken(true);
                        identityStep = tokenValue.Token;
                        if (parser.IsToken("-"))
                        {
                            parser.SkipToken(true);
                            identityStep += tokenValue.Token;
                        }
                        parser.SkipToken(false);
                        parser.ExpectedExpression(")");
                        setIdentity = true;
                    }
                    else if (parser.IsToken("CAPTION"))
                    {
                        parser.SkipToken(true);
                        caption = tokenValue.Token;
                    }
                    else
                    {
                        if (!parser.IsToken("DESCRIPTION"))
                            return !parser.EndOfText;
                        parser.SkipToken(true);
                        description = tokenValue.Token;
                    }
                }
            }
            while (parser.SkipToken(false));
            return false;
        }

        private bool ParseColumnConstraints(string columnName, SQLParser parser)
        {
            SQLParser.TokenValueClass tokenValue = parser.TokenValue;
            int rowNo = tokenValue.RowNo;
            int colNo = tokenValue.ColNo;
            string constraintName = null;
            if (parser.IsToken("CONSTRAINT"))
            {
                parser.SkipToken(true);
                constraintName = tokenValue.Token;
                parser.SkipToken(true);
            }
            if (parser.IsToken("PRIMARY"))
            {
                parser.SkipToken(true);
                parser.ExpectedExpression("KEY");
                bool clustered;
                bool asc;
                if (!parser.SkipToken(false) || !ParseColumnIndexAttributes(out clustered, out asc, parser))
                    return false;
                if (primaryKey != null)
                    throw new VistaDBSQLException(596, "", rowNo, colNo);
                if (!asc)
                    columnName = "DESC(" + columnName + ")";
                primaryKey = new PrimaryKey(this, rowNo, colNo, columnName, clustered, constraintName);
            }
            else if (parser.IsToken("UNIQUE"))
            {
                bool clustered;
                bool asc;
                if (!parser.SkipToken(false) || !ParseColumnIndexAttributes(out clustered, out asc, parser))
                    return false;
                if (!asc)
                    columnName = "DESC(" + columnName + ")";
                uniqueColumns.Add(new UniqueColumn(this, rowNo, colNo, constraintName, columnName, clustered));
                constraintName = null;
            }
            bool flag = parser.IsToken("FOREIGN");
            if (flag)
            {
                parser.SkipToken(true);
                parser.ExpectedExpression("KEY");
                parser.SkipToken(true);
                flag = true;
            }
            if (!parser.IsToken("REFERENCES"))
            {
                if (flag)
                    throw new VistaDBSQLException(507, "REFERENCES", tokenValue.RowNo, tokenValue.ColNo);
                if (constraintName != null)
                    throw new VistaDBSQLException(507, "PRIMARY KEY, FOREIGN KEY, REFERENCES", tokenValue.RowNo, tokenValue.ColNo);
                return true;
            }
            parser.SkipToken(true);
            string primaryTableName = SQLParser.TreatTemporaryTableName(tokenValue.Token, this);
            parser.SkipToken(true);
            List<string> foreignKeyNames;
            if (!ParseForeignKeyColumns(out foreignKeyNames, parser))
                return false;
            if (foreignKeyNames.Count != 1)
                throw new VistaDBSQLException(598, "", rowNo, colNo);
            VistaDBReferentialIntegrity onDelete;
            VistaDBReferentialIntegrity onUpdate;
            if (!ParseForeignKeyOptions(out onDelete, out onUpdate, parser))
                return false;
            foreignKeys.Add(new ForeignKey(this, rowNo, colNo, constraintName, new List<string>()
      {
        columnName
      }, foreignKeyNames, primaryTableName, onDelete, onUpdate));
            return !parser.EndOfText;
        }

        private bool ParseColumnIndexAttributes(out bool clustered, out bool asc, SQLParser parser)
        {
            clustered = false;
            asc = true;
            do
            {
                if (parser.IsToken("CLUSTERED"))
                    clustered = true;
                else if (parser.IsToken("NONCLUSTERED"))
                    clustered = false;
                else if (parser.IsToken("ASC"))
                {
                    asc = true;
                }
                else
                {
                    if (!parser.IsToken("DESC"))
                        return !parser.EndOfText;
                    asc = false;
                }
            }
            while (parser.SkipToken(false));
            return false;
        }

        private bool ParseForeignKeyColumns(out List<string> foreignKeyNames, SQLParser parser)
        {
            parser.ExpectedExpression("(");
            foreignKeyNames = new List<string>();
            do
            {
                parser.SkipToken(true);
                foreignKeyNames.Add(parser.TokenValue.Token);
                parser.SkipToken(true);
            }
            while (parser.IsToken(","));
            parser.ExpectedExpression(")");
            return parser.SkipToken(false);
        }

        private bool ParseForeignKeyOptions(out VistaDBReferentialIntegrity onDelete, out VistaDBReferentialIntegrity onUpdate, SQLParser parser)
        {
            SQLParser.TokenValueClass tokenValue = parser.TokenValue;
            onDelete = VistaDBReferentialIntegrity.None;
            onUpdate = VistaDBReferentialIntegrity.None;
            while (parser.IsToken("ON"))
            {
                parser.SkipToken(true);
                bool flag;
                if (parser.IsToken("DELETE"))
                {
                    flag = true;
                }
                else
                {
                    if (!parser.IsToken("UPDATE"))
                        throw new VistaDBSQLException(507, "DELETE or UPDATE", tokenValue.RowNo, tokenValue.ColNo);
                    flag = false;
                }
                parser.SkipToken(true);
                VistaDBReferentialIntegrity referentialIntegrity;
                if (parser.IsToken("NO"))
                {
                    parser.SkipToken(true);
                    parser.ExpectedExpression("ACTION");
                    referentialIntegrity = VistaDBReferentialIntegrity.None;
                }
                else if (parser.IsToken("CASCADE"))
                {
                    referentialIntegrity = VistaDBReferentialIntegrity.Cascade;
                }
                else
                {
                    if (!parser.IsToken("SET"))
                        throw new VistaDBSQLException(507, "NO ACTION, CASCADE, SET NULL or SET DEFAULT", tokenValue.RowNo, tokenValue.ColNo);
                    parser.SkipToken(true);
                    if (parser.IsToken("NULL"))
                    {
                        referentialIntegrity = VistaDBReferentialIntegrity.SetNull;
                    }
                    else
                    {
                        if (!parser.IsToken("DEFAULT"))
                            throw new VistaDBSQLException(507, "NULL or DEFAULT", tokenValue.RowNo, tokenValue.ColNo);
                        referentialIntegrity = VistaDBReferentialIntegrity.SetDefault;
                    }
                }
                if (flag)
                    onDelete = referentialIntegrity;
                else
                    onUpdate = referentialIntegrity;
                if (!parser.SkipToken(false))
                    return false;
            }
            return !parser.EndOfText;
        }

        private bool ParseTableConstraints(SQLParser parser)
        {
            SQLParser.TokenValueClass tokenValue = parser.TokenValue;
            int rowNo = tokenValue.RowNo;
            int colNo = tokenValue.ColNo;
            bool flag;
            while (true)
            {
                string str;
                if (parser.IsToken("CONSTRAINT"))
                {
                    parser.SkipToken(true);
                    str = tokenValue.Token;
                    parser.SkipToken(true);
                }
                else
                    str = null;
                bool clustered;
                string names;
                if (parser.IsToken("PRIMARY"))
                {
                    parser.SkipToken(true);
                    parser.ExpectedExpression("KEY");
                    if (parser.SkipToken(false))
                    {
                        flag = ParseTableIndexAttributes(out clustered, out names, parser);
                        if (primaryKey == null)
                            primaryKey = new PrimaryKey(this, rowNo, colNo, names, clustered, str);
                        else
                            goto label_8;
                    }
                    else
                        break;
                }
                else if (parser.IsToken("UNIQUE"))
                {
                    if (parser.SkipToken(false))
                    {
                        flag = ParseTableIndexAttributes(out clustered, out names, parser);
                        uniqueColumns.Add(new UniqueColumn(this, rowNo, colNo, str, names, clustered));
                    }
                    else
                        goto label_12;
                }
                else if (parser.IsToken("FOREIGN"))
                {
                    parser.SkipToken(true);
                    parser.ExpectedExpression("KEY");
                    parser.SkipToken(true);
                    List<string> foreignKeyNames1;
                    ParseForeignKeyColumns(out foreignKeyNames1, parser);
                    parser.ExpectedExpression("REFERENCES");
                    parser.SkipToken(true);
                    string primaryTableName = SQLParser.TreatTemporaryTableName(tokenValue.Token, this);
                    parser.SkipToken(true);
                    List<string> foreignKeyNames2;
                    ParseForeignKeyColumns(out foreignKeyNames2, parser);
                    if (foreignKeyNames1.Count == foreignKeyNames2.Count)
                    {
                        VistaDBReferentialIntegrity onDelete;
                        VistaDBReferentialIntegrity onUpdate;
                        flag = ParseForeignKeyOptions(out onDelete, out onUpdate, parser);
                        foreignKeys.Add(new ForeignKey(this, rowNo, colNo, str, foreignKeyNames1, foreignKeyNames2, primaryTableName, onDelete, onUpdate));
                    }
                    else
                        goto label_16;
                }
                else if (parser.IsToken("CHECK"))
                    flag = ParseCheckConstraint(str, parser);
                else
                    goto label_20;
                if (flag)
                {
                    if (parser.IsToken(","))
                        parser.SkipToken(true);
                    else
                        goto label_24;
                }
                else
                    goto label_22;
            }
            return false;
        label_8:
            throw new VistaDBSQLException(596, "", rowNo, colNo);
        label_12:
            return false;
        label_16:
            throw new VistaDBSQLException(598, "", rowNo, colNo);
        label_20:
            throw new VistaDBSQLException(507, "PRIMARY KEY, FOREIGN KEY, REFERENCES, CHECK", tokenValue.RowNo, tokenValue.ColNo);
        label_22:
            return flag;
        label_24:
            return true;
        }

        private bool ParseCheckConstraint(string name, SQLParser parser)
        {
            bool onDelete = false;
            bool onInsert = false;
            bool onUpdate = false;
            int rowNo = parser.TokenValue.RowNo;
            int colNo = parser.TokenValue.ColNo;
            parser.SkipToken(true);
            while (parser.IsToken("ON"))
            {
                parser.SkipToken(true);
                if (parser.IsToken("DELETE"))
                    onDelete = true;
                else if (parser.IsToken("INSERT"))
                {
                    onInsert = true;
                }
                else
                {
                    if (!parser.IsToken("UPDATE"))
                        throw new VistaDBSQLException(507, "DELETE, INSERT, UPDATE", parser.TokenValue.RowNo, parser.TokenValue.ColNo);
                    onUpdate = true;
                }
                parser.SkipToken(true);
            }
            if (!onDelete && !onInsert && !onUpdate)
            {
                onDelete = true;
                onInsert = true;
                onUpdate = true;
            }
            parser.ExpectedExpression("(");
            parser.SkipToken(true);
            int symbolNo = parser.TokenValue.SymbolNo;
            parser.NextSignature(false, true, 6);
            string expression = parser.Text.Substring(symbolNo, parser.TokenValue.SymbolNo - symbolNo).Trim();
            parser.ExpectedExpression(")");
            checks.Add(new Check(this, rowNo, colNo, name, expression, onDelete, onInsert, onUpdate));
            return parser.SkipToken(false);
        }

        private bool ParseTableIndexAttributes(out bool clustered, out string names, SQLParser parser)
        {
            if (parser.IsToken("CLUSTERED"))
            {
                parser.SkipToken(true);
                clustered = true;
            }
            else
            {
                if (parser.IsToken("NONCLUSTERED"))
                    parser.SkipToken(true);
                clustered = false;
            }
            parser.ExpectedExpression("(");
            parser.SkipToken(true);
            names = "";
            while (true)
            {
                string token = parser.TokenValue.Token;
                parser.SkipToken(true);
                if (names.Length > 0)
                    names += ";";
                if (parser.IsToken("DESC"))
                {
                    ref string local = ref names;
                    local = local + "DESC(" + token + ")";
                    parser.SkipToken(true);
                }
                else
                {
                    if (parser.IsToken("ASC"))
                        parser.SkipToken(true);
                    names += token;
                }
                if (parser.IsToken(","))
                    parser.SkipToken(true);
                else
                    break;
            }
            parser.ExpectedExpression(")");
            if (names.Length == 0)
                throw new VistaDBSQLException(507, "column name", parser.TokenValue.RowNo, parser.TokenValue.ColNo);
            parser.ExpectedExpression(")");
            return parser.SkipToken(false);
        }

        private string GenerateKeyName(string prefix, string target)
        {
            string empty = string.Empty;
            IVistaDBRelationshipCollection relationships = Database.Relationships;
            string format = "{0}_{1}";
            int num = -1;
            if (target != null)
                format += "_{2}";
            string str;
            bool flag1;
            do
            {
                bool flag2;
                do
                {
                    do
                    {
                        ++num;
                        if (num == 1)
                            format += "{3}";
                        str = string.Format(format, prefix, tableName, target, num);
                    }
                    while (primaryKey != null && connection.CompareString(primaryKey.ConstraintName, str, true) == 0);
                    flag2 = false;
                    foreach (UniqueColumn uniqueColumn in (List<UniqueColumn>)uniqueColumns)
                    {
                        if (connection.CompareString(uniqueColumn.ConstraintName, str, true) == 0)
                        {
                            flag2 = true;
                            break;
                        }
                    }
                }
                while (flag2);
                flag1 = false;
                foreach (KeyValuePair<string, ForeignKey> foreignKey in (Dictionary<string, ForeignKey>)foreignKeys)
                {
                    if (connection.CompareString(foreignKey.Key, str, true) == 0 || connection.CompareString(foreignKey.Value.ConstraintName, str, true) == 0)
                    {
                        flag1 = true;
                        break;
                    }
                }
            }
            while (flag1 || relationships.ContainsKey(str));
            return str;
        }

        protected override VistaDBType OnPrepareQuery()
        {
            uniqueColumns.Prepare();
            foreignKeys.Prepare();
            checks.Prepare();
            if (primaryKey != null)
                primaryKey.Prepare();
            return VistaDBType.Unknown;
        }

        protected void AddColumns(IVistaDBTableSchema tableSchema)
        {
            foreach (ColumnDescr column in (List<ColumnDescr>)columns)
            {
                if (Utils.IsCharacterDataType(column.DataType))
                    tableSchema.AddColumn(column.ColumnName, column.DataType, column.Width, column.CodePage);
                else
                    tableSchema.AddColumn(column.ColumnName, column.DataType);
                tableSchema.DefineColumnAttributes(column.ColumnName, column.AllowNull, column.ReadOnly, column.Encrypted, column.Packed, column.Description);
                if (column.SetIdentity)
                    tableSchema.DefineIdentity(column.ColumnName, column.IdentitySeed, column.IdentityStep);
                else if (column.DefaultValue != null)
                    tableSchema.DefineDefaultValue(column.ColumnName, column.DefaultValue, column.DefaultValueUseInUpdate, column.DefaultValueDescription);
            }
        }

        protected void AddConstraints(IVistaDBTableSchema tableSchema)
        {
            if (primaryKey != null)
                tableSchema.DefineIndex(primaryKey.ConstraintName, primaryKey.PrimaryKeyNames, true, true);
            foreach (UniqueColumn uniqueColumn in (List<UniqueColumn>)uniqueColumns)
                tableSchema.DefineIndex(uniqueColumn.ConstraintName, uniqueColumn.ColumnNames, false, true);
            foreach (Check check in (List<Check>)checks)
                tableSchema.DefineConstraint(check.ConstraintName, check.Expression, check.Description, check.OnInsert, check.OnUpdate, check.OnDelete);
        }

        protected void AddForeignKeys(IVistaDBTable table)
        {
            foreach (ForeignKey foreignKey in foreignKeys.Values)
                table.CreateForeignKey(foreignKey.ConstraintName, foreignKey.GetExpression(), foreignKey.PrimaryTableName, foreignKey.OnUpdate, foreignKey.OnDelete, foreignKey.Description);
        }

        protected class ColumnDescr
        {
            private BaseCreateTableStatement parent;
            private int lineNo;
            private int symbolNo;
            private string columnName;
            private VistaDBType dataType;
            private int width;
            private int codePage;
            private bool allowNull;
            private bool encrypted;
            private bool packed;
            private bool setIdentity;
            private string identitySeed;
            private string identityStep;
            private string caption;
            private string description;
            private bool readOnly;
            private string defaultValue;
            private bool defaultValueUseInUpdate;
            private string defaultValueDescription;

            public ColumnDescr(BaseCreateTableStatement parent, int lineNo, int symbolNo, string columnName, VistaDBType dataType, int width, int codePage, bool allowNull, bool readOnly, bool encrypted, bool packed, string defaultValue, bool setIdentity, string identitySeed, string identityStep, string caption, string description)
            {
                if (defaultValue != null && setIdentity)
                    throw new VistaDBSQLException(599, columnName, lineNo, symbolNo);
                this.parent = parent;
                this.lineNo = lineNo;
                this.symbolNo = symbolNo;
                this.columnName = columnName;
                this.dataType = dataType;
                this.width = width;
                this.codePage = codePage;
                this.allowNull = allowNull;
                this.readOnly = readOnly;
                this.encrypted = encrypted;
                this.packed = packed;
                this.defaultValue = defaultValue;
                this.setIdentity = setIdentity;
                this.identitySeed = identitySeed;
                this.identityStep = identityStep;
                this.caption = caption;
                this.description = description;
                defaultValueUseInUpdate = false;
                defaultValueDescription = null;
            }

            public int LineNo
            {
                get
                {
                    return lineNo;
                }
            }

            public int SymbolNo
            {
                get
                {
                    return symbolNo;
                }
            }

            public string ColumnName
            {
                get
                {
                    return columnName;
                }
            }

            public VistaDBType DataType
            {
                get
                {
                    return dataType;
                }
            }

            public int Width
            {
                get
                {
                    return width;
                }
            }

            public int CodePage
            {
                get
                {
                    if (codePage <= 0)
                        codePage = parent.Database.Culture.TextInfo.ANSICodePage;
                    return codePage;
                }
            }

            public bool AllowNull
            {
                get
                {
                    return allowNull;
                }
            }

            public bool ReadOnly
            {
                get
                {
                    return readOnly;
                }
            }

            public bool Encrypted
            {
                get
                {
                    return encrypted;
                }
            }

            public bool Packed
            {
                get
                {
                    return packed;
                }
            }

            public string DefaultValue
            {
                get
                {
                    return defaultValue;
                }
            }

            public bool SetIdentity
            {
                get
                {
                    return setIdentity;
                }
            }

            public string IdentitySeed
            {
                get
                {
                    return identitySeed;
                }
            }

            public string IdentityStep
            {
                get
                {
                    return identityStep;
                }
            }

            public string Caption
            {
                get
                {
                    return caption;
                }
            }

            public string Description
            {
                get
                {
                    return description;
                }
            }

            public bool DefaultValueUseInUpdate
            {
                get
                {
                    return defaultValueUseInUpdate;
                }
            }

            public string DefaultValueDescription
            {
                get
                {
                    return defaultValueDescription;
                }
            }
        }

        protected class ColumnDescrList : List<ColumnDescr>
        {
            private Statement parent;

            public ColumnDescrList(Statement parent)
            {
                this.parent = parent;
            }

            new public int Add(ColumnDescr column)
            {
                foreach (ColumnDescr columnDescr in (List<ColumnDescr>)this)
                {
                    if (parent.Connection.CompareString(columnDescr.ColumnName, column.ColumnName, true) == 0)
                        throw new VistaDBSQLException(594, column.ColumnName, column.LineNo, column.SymbolNo);
                }
                base.Add(column);
                return Count - 1;
            }
        }

        protected class ForeignKey
        {
            private BaseCreateTableStatement parent;
            private int lineNo;
            private int symbolNo;
            private string constraintName;
            private List<string> foreignKeyNames;
            private List<string> primaryKeyNames;
            private string primaryTableName;
            private VistaDBReferentialIntegrity onDelete;
            private VistaDBReferentialIntegrity onUpdate;

            public ForeignKey(BaseCreateTableStatement parent, int lineNo, int symbolNo, string constraintName, List<string> foreignKeyNames, List<string> primaryKeyNames, string primaryTableName, VistaDBReferentialIntegrity onDelete, VistaDBReferentialIntegrity onUpdate)
            {
                this.parent = parent;
                this.lineNo = lineNo;
                this.symbolNo = symbolNo;
                this.constraintName = constraintName;
                this.foreignKeyNames = foreignKeyNames;
                this.primaryKeyNames = primaryKeyNames;
                this.primaryTableName = primaryTableName;
                this.onDelete = onDelete;
                this.onUpdate = onUpdate;
            }

            public void Prepare()
            {
                IVistaDBTableSchema vistaDbTableSchema = parent.Database.TableSchema(primaryTableName);
                IVistaDBKeyColumn[] vistaDbKeyColumnArray = null;
                IRow rowStructure = parent.Database.GetRowStructure(primaryTableName);
                foreach (IVistaDBIndexInformation indexInformation in (IEnumerable<IVistaDBIndexInformation>)vistaDbTableSchema.Indexes.Values)
                {
                    if (indexInformation.Primary)
                    {
                        vistaDbKeyColumnArray = indexInformation.KeyStructure;
                        break;
                    }
                }
                if (vistaDbKeyColumnArray == null)
                    throw new VistaDBSQLException(600, "", lineNo, symbolNo);
                for (int index = 0; index < vistaDbKeyColumnArray.Length; ++index)
                {
                    if (parent.Connection.CompareString(rowStructure[vistaDbKeyColumnArray[index].RowIndex].Name, primaryKeyNames[index], true) != 0)
                        throw new VistaDBSQLException(601, "", lineNo, symbolNo);
                }
                if (constraintName != null)
                    return;
                constraintName = parent.GenerateKeyName("FK", primaryTableName);
            }

            public string GetExpression()
            {
                StringBuilder stringBuilder = new StringBuilder();
                bool flag = true;
                foreach (string foreignKeyName in foreignKeyNames)
                {
                    if (!flag)
                        stringBuilder.Append(";");
                    stringBuilder.Append(foreignKeyName);
                    flag = false;
                }
                return stringBuilder.ToString();
            }

            public int LineNo
            {
                get
                {
                    return lineNo;
                }
            }

            public int SymbolNo
            {
                get
                {
                    return symbolNo;
                }
            }

            public string ConstraintName
            {
                get
                {
                    return constraintName;
                }
            }

            public List<string> ForeignKeyNames
            {
                get
                {
                    return foreignKeyNames;
                }
            }

            public List<string> PrimaryKeyNames
            {
                get
                {
                    return primaryKeyNames;
                }
            }

            public string PrimaryTableName
            {
                get
                {
                    return primaryTableName;
                }
            }

            public VistaDBReferentialIntegrity OnDelete
            {
                get
                {
                    return onDelete;
                }
            }

            public VistaDBReferentialIntegrity OnUpdate
            {
                get
                {
                    return onUpdate;
                }
            }

            public string Description
            {
                get
                {
                    return null;
                }
            }
        }

        protected class ForeignKeyList : Dictionary<string, ForeignKey>
        {
            private BaseCreateTableStatement parent;

            public ForeignKeyList(BaseCreateTableStatement parent)
              : base(StringComparer.OrdinalIgnoreCase)
            {
                this.parent = parent;
            }

            public int Add(ForeignKey foreignKey)
            {
                string key = foreignKey.ConstraintName ?? "$$$tmp_fkcn_" + Count.ToString("000");
                if (ContainsKey(key))
                    throw new VistaDBSQLException(595, foreignKey.ConstraintName, foreignKey.LineNo, foreignKey.SymbolNo);
                Add(key, foreignKey);
                return Count - 1;
            }

            public void Prepare()
            {
                foreach (ForeignKey foreignKey in Values)
                    foreignKey.Prepare();
            }
        }

        protected class PrimaryKey
        {
            private BaseCreateTableStatement parent;
            private int lineNo;
            private int symbolNo;
            private string primaryKeyNames;
            private bool clustered;
            private string constraintName;

            public PrimaryKey(BaseCreateTableStatement parent, int lineNo, int symbolNo, string primaryKeyNames, bool clustered, string constraintName)
            {
                if (constraintName != null || clustered)
                {
                    foreach (UniqueColumn uniqueColumn in (List<UniqueColumn>)parent.uniqueColumns)
                    {
                        if (constraintName != null && parent.Connection.CompareString(constraintName, uniqueColumn.ConstraintName, true) == 0)
                            throw new VistaDBSQLException(595, constraintName, lineNo, symbolNo);
                        if (uniqueColumn.Clustered)
                            throw new VistaDBSQLException(597, "DROP existing clustered index.", lineNo, symbolNo);
                    }
                }
                this.parent = parent;
                this.lineNo = lineNo;
                this.symbolNo = symbolNo;
                this.primaryKeyNames = primaryKeyNames;
                this.clustered = clustered;
                this.constraintName = constraintName;
            }

            public void Prepare()
            {
                if (constraintName != null)
                    return;
                constraintName = parent.GenerateKeyName("PK", null);
            }

            public string PrimaryKeyNames
            {
                get
                {
                    return primaryKeyNames;
                }
            }

            public bool Clustered
            {
                get
                {
                    return clustered;
                }
            }

            public string ConstraintName
            {
                get
                {
                    return constraintName;
                }
            }
        }

        protected class UniqueColumn
        {
            private BaseCreateTableStatement parent;
            private int lineNo;
            private int symbolNo;
            private string constraintName;
            private string columnNames;
            private bool clustered;

            public UniqueColumn(BaseCreateTableStatement parent, int lineNo, int symbolNo, string constraintName, string columnNames, bool clustered)
            {
                this.parent = parent;
                this.lineNo = lineNo;
                this.symbolNo = symbolNo;
                this.constraintName = constraintName;
                this.columnNames = columnNames;
                this.clustered = clustered;
            }

            public void Prepare()
            {
                if (constraintName != null)
                    return;
                constraintName = parent.GenerateKeyName("UN", null);
            }

            public int LineNo
            {
                get
                {
                    return lineNo;
                }
            }

            public int SymbolNo
            {
                get
                {
                    return symbolNo;
                }
            }

            public string ColumnNames
            {
                get
                {
                    return columnNames;
                }
            }

            public bool Clustered
            {
                get
                {
                    return clustered;
                }
            }

            public string ConstraintName
            {
                get
                {
                    return constraintName;
                }
            }
        }

        protected class UniqueColumnList : List<UniqueColumn>
        {
            private BaseCreateTableStatement parent;

            public UniqueColumnList(BaseCreateTableStatement parent)
            {
                this.parent = parent;
            }

            new public int Add(UniqueColumn column)
            {
                if (column.ConstraintName != null || column.Clustered)
                {
                    foreach (UniqueColumn uniqueColumn in (List<UniqueColumn>)this)
                    {
                        if (column.ConstraintName != null && parent.connection.CompareString(uniqueColumn.ConstraintName, column.ConstraintName, true) == 0)
                            throw new VistaDBSQLException(595, column.ConstraintName, column.LineNo, column.SymbolNo);
                        if (column.Clustered && uniqueColumn.Clustered)
                            throw new VistaDBSQLException(597, "Drop existing clustered index.", column.LineNo, column.SymbolNo);
                    }
                    if (parent.primaryKey != null && parent.connection.CompareString(parent.primaryKey.ConstraintName, column.ConstraintName, true) == 0)
                        throw new VistaDBSQLException(595, column.ConstraintName, column.LineNo, column.SymbolNo);
                }
                base.Add(column);
                return Count - 1;
            }

            public void Prepare()
            {
                foreach (UniqueColumn uniqueColumn in (List<UniqueColumn>)this)
                    uniqueColumn.Prepare();
            }
        }

        protected class Check
        {
            private BaseCreateTableStatement parent;
            private int lineNo;
            private int symbolNo;
            private string constraintName;
            private string expression;
            private bool onDelete;
            private bool onInsert;
            private bool onUpdate;

            public Check(BaseCreateTableStatement parent, int lineNo, int symbolNo, string constraintName, string expression, bool onDelete, bool onInsert, bool onUpdate)
            {
                this.parent = parent;
                this.lineNo = lineNo;
                this.symbolNo = symbolNo;
                this.constraintName = constraintName;
                this.expression = expression;
                this.onDelete = onDelete;
                this.onInsert = onInsert;
                this.onUpdate = onUpdate;
            }

            public void Prepare()
            {
                if (constraintName != null)
                    return;
                constraintName = parent.GenerateKeyName("CK", null);
            }

            public int LineNo
            {
                get
                {
                    return lineNo;
                }
            }

            public int SymbolNo
            {
                get
                {
                    return symbolNo;
                }
            }

            public string ConstraintName
            {
                get
                {
                    return constraintName;
                }
            }

            public string Expression
            {
                get
                {
                    return expression;
                }
            }

            public string Description
            {
                get
                {
                    return null;
                }
            }

            public bool OnDelete
            {
                get
                {
                    return onDelete;
                }
            }

            public bool OnInsert
            {
                get
                {
                    return onInsert;
                }
            }

            public bool OnUpdate
            {
                get
                {
                    return onUpdate;
                }
            }
        }

        protected class CheckList : List<Check>
        {
            private Statement parent;

            public CheckList(Statement parent)
            {
                this.parent = parent;
            }

            new public int Add(Check check)
            {
                if (check.ConstraintName != null)
                {
                    foreach (Check check1 in (List<Check>)this)
                    {
                        if (parent.Connection.CompareString(check1.ConstraintName, check.ConstraintName, true) == 0)
                            throw new VistaDBSQLException(595, check.ConstraintName, check.LineNo, check.SymbolNo);
                    }
                }
                base.Add(check);
                return Count - 1;
            }

            public void Prepare()
            {
                foreach (Check check in (List<Check>)this)
                    check.Prepare();
            }
        }
    }
}
