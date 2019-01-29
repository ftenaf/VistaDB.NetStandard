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
    protected BaseCreateTableStatement.PrimaryKey primaryKey;
    protected string tableDescription;
    protected string tableName;
    protected BaseCreateTableStatement.ColumnDescrList columns;
    protected BaseCreateTableStatement.UniqueColumnList uniqueColumns;
    protected BaseCreateTableStatement.ForeignKeyList foreignKeys;
    protected BaseCreateTableStatement.CheckList checks;

    protected BaseCreateTableStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void DoBeforeParse()
    {
      base.DoBeforeParse();
      this.columns = new BaseCreateTableStatement.ColumnDescrList((Statement) this);
      this.uniqueColumns = new BaseCreateTableStatement.UniqueColumnList(this);
      this.foreignKeys = new BaseCreateTableStatement.ForeignKeyList(this);
      this.checks = new BaseCreateTableStatement.CheckList((Statement) this);
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      if (this.id < 0L)
        return;
      parser.SkipToken(true);
      this.tableName = parser.GetTableName((Statement) this);
      parser.SkipToken(true);
    }

    protected bool ParseColumns(SQLParser parser)
    {
      SQLParser.TokenValueClass tokenValue = parser.TokenValue;
      do
      {
        parser.SkipToken(true);
        if (tokenValue.TokenType == TokenType.Unknown && (parser.IsToken("CONSTRAINT") || parser.IsToken("CHECK") || (parser.IsToken("PRIMARY") || parser.IsToken("UNIQUE")) || parser.IsToken("FOREIGN")))
          return this.ParseTableConstraints(parser);
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
        this.ParseColumnAttributes(out columnName, out dataType, out width, out codePage, out allowNull, out readOnly, out encrypted, out packed, out defaultValue, out setIdentity, out identitySeed, out identityStep, out caption, out description, parser);
        if (dataType == VistaDBType.Unknown)
          throw new VistaDBSQLException(508, tokenValue.Token, tokenValue.RowNo, tokenValue.ColNo);
        bool columnConstraints = this.ParseColumnConstraints(columnName, parser);
        if (parser.IsToken("ROWGUIDCOL"))
        {
          if (dataType != VistaDBType.UniqueIdentifier)
            throw new VistaDBSQLException(655, tokenValue.Token, tokenValue.RowNo, tokenValue.ColNo);
          defaultValue = "NEWID" + "()";
          readOnly = true;
          parser.SkipToken(false);
        }
        this.columns.Add(new BaseCreateTableStatement.ColumnDescr(this, rowNo, colNo, columnName, dataType, width, codePage, allowNull, readOnly, encrypted, packed, defaultValue, setIdentity, identitySeed, identityStep, caption, description));
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
      columnName = (string) null;
      dataType = VistaDBType.Unknown;
      width = 30;
      codePage = 0;
      allowNull = true;
      readOnly = false;
      encrypted = false;
      packed = false;
      defaultValue = (string) null;
      setIdentity = false;
      identitySeed = (string) null;
      identityStep = (string) null;
      caption = (string) null;
      description = (string) null;
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
        codePage = BaseCreateStatement.StrTokenToInt(parser);
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
      string constraintName = (string) null;
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
        if (!parser.SkipToken(false) || !this.ParseColumnIndexAttributes(out clustered, out asc, parser))
          return false;
        if (this.primaryKey != null)
          throw new VistaDBSQLException(596, "", rowNo, colNo);
        if (!asc)
          columnName = "DESC(" + columnName + ")";
        this.primaryKey = new BaseCreateTableStatement.PrimaryKey(this, rowNo, colNo, columnName, clustered, constraintName);
      }
      else if (parser.IsToken("UNIQUE"))
      {
        bool clustered;
        bool asc;
        if (!parser.SkipToken(false) || !this.ParseColumnIndexAttributes(out clustered, out asc, parser))
          return false;
        if (!asc)
          columnName = "DESC(" + columnName + ")";
        this.uniqueColumns.Add(new BaseCreateTableStatement.UniqueColumn(this, rowNo, colNo, constraintName, columnName, clustered));
        constraintName = (string) null;
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
      string primaryTableName = SQLParser.TreatTemporaryTableName(tokenValue.Token, (Statement) this);
      parser.SkipToken(true);
      List<string> foreignKeyNames;
      if (!this.ParseForeignKeyColumns(out foreignKeyNames, parser))
        return false;
      if (foreignKeyNames.Count != 1)
        throw new VistaDBSQLException(598, "", rowNo, colNo);
      VistaDBReferentialIntegrity onDelete;
      VistaDBReferentialIntegrity onUpdate;
      if (!this.ParseForeignKeyOptions(out onDelete, out onUpdate, parser))
        return false;
      this.foreignKeys.Add(new BaseCreateTableStatement.ForeignKey(this, rowNo, colNo, constraintName, new List<string>()
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
          str = (string) null;
        bool clustered;
        string names;
        if (parser.IsToken("PRIMARY"))
        {
          parser.SkipToken(true);
          parser.ExpectedExpression("KEY");
          if (parser.SkipToken(false))
          {
            flag = this.ParseTableIndexAttributes(out clustered, out names, parser);
            if (this.primaryKey == null)
              this.primaryKey = new BaseCreateTableStatement.PrimaryKey(this, rowNo, colNo, names, clustered, str);
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
            flag = this.ParseTableIndexAttributes(out clustered, out names, parser);
            this.uniqueColumns.Add(new BaseCreateTableStatement.UniqueColumn(this, rowNo, colNo, str, names, clustered));
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
          this.ParseForeignKeyColumns(out foreignKeyNames1, parser);
          parser.ExpectedExpression("REFERENCES");
          parser.SkipToken(true);
          string primaryTableName = SQLParser.TreatTemporaryTableName(tokenValue.Token, (Statement) this);
          parser.SkipToken(true);
          List<string> foreignKeyNames2;
          this.ParseForeignKeyColumns(out foreignKeyNames2, parser);
          if (foreignKeyNames1.Count == foreignKeyNames2.Count)
          {
            VistaDBReferentialIntegrity onDelete;
            VistaDBReferentialIntegrity onUpdate;
            flag = this.ParseForeignKeyOptions(out onDelete, out onUpdate, parser);
            this.foreignKeys.Add(new BaseCreateTableStatement.ForeignKey(this, rowNo, colNo, str, foreignKeyNames1, foreignKeyNames2, primaryTableName, onDelete, onUpdate));
          }
          else
            goto label_16;
        }
        else if (parser.IsToken("CHECK"))
          flag = this.ParseCheckConstraint(str, parser);
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
      this.checks.Add(new BaseCreateTableStatement.Check(this, rowNo, colNo, name, expression, onDelete, onInsert, onUpdate));
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
      IVistaDBRelationshipCollection relationships = this.Database.Relationships;
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
            str = string.Format(format, (object) prefix, (object) this.tableName, (object) target, (object) num);
          }
          while (this.primaryKey != null && this.connection.CompareString(this.primaryKey.ConstraintName, str, true) == 0);
          flag2 = false;
          foreach (BaseCreateTableStatement.UniqueColumn uniqueColumn in (List<BaseCreateTableStatement.UniqueColumn>) this.uniqueColumns)
          {
            if (this.connection.CompareString(uniqueColumn.ConstraintName, str, true) == 0)
            {
              flag2 = true;
              break;
            }
          }
        }
        while (flag2);
        flag1 = false;
        foreach (KeyValuePair<string, BaseCreateTableStatement.ForeignKey> foreignKey in (Dictionary<string, BaseCreateTableStatement.ForeignKey>) this.foreignKeys)
        {
          if (this.connection.CompareString(foreignKey.Key, str, true) == 0 || this.connection.CompareString(foreignKey.Value.ConstraintName, str, true) == 0)
          {
            flag1 = true;
            break;
          }
        }
      }
      while (flag1 || relationships.ContainsKey(str));
      return str;
    }

    private int NameIndex
    {
      get
      {
        return this.nameIndex++;
      }
    }

    protected override VistaDBType OnPrepareQuery()
    {
      this.uniqueColumns.Prepare();
      this.foreignKeys.Prepare();
      this.checks.Prepare();
      if (this.primaryKey != null)
        this.primaryKey.Prepare();
      return VistaDBType.Unknown;
    }

    protected void AddColumns(IVistaDBTableSchema tableSchema)
    {
      foreach (BaseCreateTableStatement.ColumnDescr column in (List<BaseCreateTableStatement.ColumnDescr>) this.columns)
      {
        if (Utils.IsCharacterDataType(column.DataType))
          tableSchema.AddColumn(column.ColumnName, column.DataType, column.Width, column.CodePage);
        else
          tableSchema.AddColumn(column.ColumnName, column.DataType);
        tableSchema.DefineColumnAttributes(column.ColumnName, column.AllowNull, column.ReadOnly, column.Encrypted, column.Packed, column.Caption, column.Description);
        if (column.SetIdentity)
          tableSchema.DefineIdentity(column.ColumnName, column.IdentitySeed, column.IdentityStep);
        else if (column.DefaultValue != null)
          tableSchema.DefineDefaultValue(column.ColumnName, column.DefaultValue, column.DefaultValueUseInUpdate, column.DefaultValueDescription);
      }
    }

    protected void AddConstraints(IVistaDBTableSchema tableSchema)
    {
      if (this.primaryKey != null)
        tableSchema.DefineIndex(this.primaryKey.ConstraintName, this.primaryKey.PrimaryKeyNames, true, true);
      foreach (BaseCreateTableStatement.UniqueColumn uniqueColumn in (List<BaseCreateTableStatement.UniqueColumn>) this.uniqueColumns)
        tableSchema.DefineIndex(uniqueColumn.ConstraintName, uniqueColumn.ColumnNames, false, true);
      foreach (BaseCreateTableStatement.Check check in (List<BaseCreateTableStatement.Check>) this.checks)
        tableSchema.DefineConstraint(check.ConstraintName, check.Expression, check.Description, check.OnInsert, check.OnUpdate, check.OnDelete);
    }

    protected void AddForeignKeys(IVistaDBTable table)
    {
      foreach (BaseCreateTableStatement.ForeignKey foreignKey in this.foreignKeys.Values)
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
        this.defaultValueUseInUpdate = false;
        this.defaultValueDescription = (string) null;
      }

      public int LineNo
      {
        get
        {
          return this.lineNo;
        }
      }

      public int SymbolNo
      {
        get
        {
          return this.symbolNo;
        }
      }

      public string ColumnName
      {
        get
        {
          return this.columnName;
        }
      }

      public VistaDBType DataType
      {
        get
        {
          return this.dataType;
        }
      }

      public int Width
      {
        get
        {
          return this.width;
        }
      }

      public int CodePage
      {
        get
        {
          if (this.codePage <= 0)
            this.codePage = this.parent.Database.Culture.TextInfo.ANSICodePage;
          return this.codePage;
        }
      }

      public bool AllowNull
      {
        get
        {
          return this.allowNull;
        }
      }

      public bool ReadOnly
      {
        get
        {
          return this.readOnly;
        }
      }

      public bool Encrypted
      {
        get
        {
          return this.encrypted;
        }
      }

      public bool Packed
      {
        get
        {
          return this.packed;
        }
      }

      public string DefaultValue
      {
        get
        {
          return this.defaultValue;
        }
      }

      public bool SetIdentity
      {
        get
        {
          return this.setIdentity;
        }
      }

      public string IdentitySeed
      {
        get
        {
          return this.identitySeed;
        }
      }

      public string IdentityStep
      {
        get
        {
          return this.identityStep;
        }
      }

      public string Caption
      {
        get
        {
          return this.caption;
        }
      }

      public string Description
      {
        get
        {
          return this.description;
        }
      }

      public bool DefaultValueUseInUpdate
      {
        get
        {
          return this.defaultValueUseInUpdate;
        }
      }

      public string DefaultValueDescription
      {
        get
        {
          return this.defaultValueDescription;
        }
      }
    }

    protected class ColumnDescrList : List<BaseCreateTableStatement.ColumnDescr>
    {
      private Statement parent;

      public ColumnDescrList(Statement parent)
      {
        this.parent = parent;
      }

      public int Add(BaseCreateTableStatement.ColumnDescr column)
      {
        foreach (BaseCreateTableStatement.ColumnDescr columnDescr in (List<BaseCreateTableStatement.ColumnDescr>) this)
        {
          if (this.parent.Connection.CompareString(columnDescr.ColumnName, column.ColumnName, true) == 0)
            throw new VistaDBSQLException(594, column.ColumnName, column.LineNo, column.SymbolNo);
        }
        base.Add(column);
        return this.Count - 1;
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
        IVistaDBTableSchema vistaDbTableSchema = this.parent.Database.TableSchema(this.primaryTableName);
        IVistaDBKeyColumn[] vistaDbKeyColumnArray = (IVistaDBKeyColumn[]) null;
        IRow rowStructure = this.parent.Database.GetRowStructure(this.primaryTableName);
        foreach (IVistaDBIndexInformation indexInformation in (IEnumerable<IVistaDBIndexInformation>) vistaDbTableSchema.Indexes.Values)
        {
          if (indexInformation.Primary)
          {
            vistaDbKeyColumnArray = indexInformation.KeyStructure;
            break;
          }
        }
        if (vistaDbKeyColumnArray == null)
          throw new VistaDBSQLException(600, "", this.lineNo, this.symbolNo);
        for (int index = 0; index < vistaDbKeyColumnArray.Length; ++index)
        {
          if (this.parent.Connection.CompareString(rowStructure[vistaDbKeyColumnArray[index].RowIndex].Name, this.primaryKeyNames[index], true) != 0)
            throw new VistaDBSQLException(601, "", this.lineNo, this.symbolNo);
        }
        if (this.constraintName != null)
          return;
        this.constraintName = this.parent.GenerateKeyName("FK", this.primaryTableName);
      }

      public string GetExpression()
      {
        StringBuilder stringBuilder = new StringBuilder();
        bool flag = true;
        foreach (string foreignKeyName in this.foreignKeyNames)
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
          return this.lineNo;
        }
      }

      public int SymbolNo
      {
        get
        {
          return this.symbolNo;
        }
      }

      public string ConstraintName
      {
        get
        {
          return this.constraintName;
        }
      }

      public List<string> ForeignKeyNames
      {
        get
        {
          return this.foreignKeyNames;
        }
      }

      public List<string> PrimaryKeyNames
      {
        get
        {
          return this.primaryKeyNames;
        }
      }

      public string PrimaryTableName
      {
        get
        {
          return this.primaryTableName;
        }
      }

      public VistaDBReferentialIntegrity OnDelete
      {
        get
        {
          return this.onDelete;
        }
      }

      public VistaDBReferentialIntegrity OnUpdate
      {
        get
        {
          return this.onUpdate;
        }
      }

      public string Description
      {
        get
        {
          return (string) null;
        }
      }
    }

    protected class ForeignKeyList : Dictionary<string, BaseCreateTableStatement.ForeignKey>
    {
      private BaseCreateTableStatement parent;

      public ForeignKeyList(BaseCreateTableStatement parent)
        : base((IEqualityComparer<string>) StringComparer.OrdinalIgnoreCase)
      {
        this.parent = parent;
      }

      public int Add(BaseCreateTableStatement.ForeignKey foreignKey)
      {
        string key = foreignKey.ConstraintName ?? "$$$tmp_fkcn_" + this.Count.ToString("000");
        if (this.ContainsKey(key))
          throw new VistaDBSQLException(595, foreignKey.ConstraintName, foreignKey.LineNo, foreignKey.SymbolNo);
        this.Add(key, foreignKey);
        return this.Count - 1;
      }

      public void Prepare()
      {
        foreach (BaseCreateTableStatement.ForeignKey foreignKey in this.Values)
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
          foreach (BaseCreateTableStatement.UniqueColumn uniqueColumn in (List<BaseCreateTableStatement.UniqueColumn>) parent.uniqueColumns)
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
        if (this.constraintName != null)
          return;
        this.constraintName = this.parent.GenerateKeyName("PK", (string) null);
      }

      public string PrimaryKeyNames
      {
        get
        {
          return this.primaryKeyNames;
        }
      }

      public bool Clustered
      {
        get
        {
          return this.clustered;
        }
      }

      public string ConstraintName
      {
        get
        {
          return this.constraintName;
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
        if (this.constraintName != null)
          return;
        this.constraintName = this.parent.GenerateKeyName("UN", (string) null);
      }

      public int LineNo
      {
        get
        {
          return this.lineNo;
        }
      }

      public int SymbolNo
      {
        get
        {
          return this.symbolNo;
        }
      }

      public string ColumnNames
      {
        get
        {
          return this.columnNames;
        }
      }

      public bool Clustered
      {
        get
        {
          return this.clustered;
        }
      }

      public string ConstraintName
      {
        get
        {
          return this.constraintName;
        }
      }
    }

    protected class UniqueColumnList : List<BaseCreateTableStatement.UniqueColumn>
    {
      private BaseCreateTableStatement parent;

      public UniqueColumnList(BaseCreateTableStatement parent)
      {
        this.parent = parent;
      }

      public int Add(BaseCreateTableStatement.UniqueColumn column)
      {
        if (column.ConstraintName != null || column.Clustered)
        {
          foreach (BaseCreateTableStatement.UniqueColumn uniqueColumn in (List<BaseCreateTableStatement.UniqueColumn>) this)
          {
            if (column.ConstraintName != null && this.parent.connection.CompareString(uniqueColumn.ConstraintName, column.ConstraintName, true) == 0)
              throw new VistaDBSQLException(595, column.ConstraintName, column.LineNo, column.SymbolNo);
            if (column.Clustered && uniqueColumn.Clustered)
              throw new VistaDBSQLException(597, "Drop existing clustered index.", column.LineNo, column.SymbolNo);
          }
          if (this.parent.primaryKey != null && this.parent.connection.CompareString(this.parent.primaryKey.ConstraintName, column.ConstraintName, true) == 0)
            throw new VistaDBSQLException(595, column.ConstraintName, column.LineNo, column.SymbolNo);
        }
        base.Add(column);
        return this.Count - 1;
      }

      public void Prepare()
      {
        foreach (BaseCreateTableStatement.UniqueColumn uniqueColumn in (List<BaseCreateTableStatement.UniqueColumn>) this)
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
        if (this.constraintName != null)
          return;
        this.constraintName = this.parent.GenerateKeyName("CK", (string) null);
      }

      public int LineNo
      {
        get
        {
          return this.lineNo;
        }
      }

      public int SymbolNo
      {
        get
        {
          return this.symbolNo;
        }
      }

      public string ConstraintName
      {
        get
        {
          return this.constraintName;
        }
      }

      public string Expression
      {
        get
        {
          return this.expression;
        }
      }

      public string Description
      {
        get
        {
          return (string) null;
        }
      }

      public bool OnDelete
      {
        get
        {
          return this.onDelete;
        }
      }

      public bool OnInsert
      {
        get
        {
          return this.onInsert;
        }
      }

      public bool OnUpdate
      {
        get
        {
          return this.onUpdate;
        }
      }
    }

    protected class CheckList : List<BaseCreateTableStatement.Check>
    {
      private Statement parent;

      public CheckList(Statement parent)
      {
        this.parent = parent;
      }

      public int Add(BaseCreateTableStatement.Check check)
      {
        if (check.ConstraintName != null)
        {
          foreach (BaseCreateTableStatement.Check check1 in (List<BaseCreateTableStatement.Check>) this)
          {
            if (this.parent.Connection.CompareString(check1.ConstraintName, check.ConstraintName, true) == 0)
              throw new VistaDBSQLException(595, check.ConstraintName, check.LineNo, check.SymbolNo);
          }
        }
        base.Add(check);
        return this.Count - 1;
      }

      public void Prepare()
      {
        foreach (BaseCreateTableStatement.Check check in (List<BaseCreateTableStatement.Check>) this)
          check.Prepare();
      }
    }
  }
}
