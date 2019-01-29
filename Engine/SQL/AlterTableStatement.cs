using System.Collections.Generic;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class AlterTableStatement : BaseCreateTableStatement
  {
    private AlterTableStatement.AlterType alterType;
    private BaseCreateTableStatement.ColumnDescr alterColumn;
    private List<AlterTableStatement.DropItem> dropItems;
    private bool activateSyncService;

    public AlterTableStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      base.OnParse(connection, parser);
      if (parser.IsToken("SYNCHRONIZATION"))
      {
        parser.SkipToken(true);
        if (parser.IsToken("ON"))
        {
          this.activateSyncService = true;
        }
        else
        {
          parser.ExpectedExpression("OFF");
          this.activateSyncService = false;
        }
        this.alterType = AlterTableStatement.AlterType.SyncService;
        parser.SkipToken(false);
      }
      else if (parser.IsToken("DESCRIPTION"))
        this.ParseNewDescription(parser);
      else if (parser.IsToken("ALTER"))
        this.ParseAlterColumn(parser);
      else if (parser.IsToken("ADD"))
      {
        this.ParseAddColumnOrConstraint(parser);
      }
      else
      {
        if (!parser.IsToken("DROP"))
          throw new VistaDBSQLException(507, "DESCRIPTION, ALTER COLUMN, ADD, DROP, SYNCHRONIZATION", parser.TokenValue.RowNo, parser.TokenValue.ColNo);
        this.ParseDropColumnOrConstraint(parser);
      }
    }

    private void ParseNewDescription(SQLParser parser)
    {
      this.alterType = AlterTableStatement.AlterType.NewDescription;
      parser.SkipToken(true);
      this.tableDescription = parser.TokenValue.Token;
      parser.SkipToken(false);
    }

    private void ParseAlterColumn(SQLParser parser)
    {
      this.alterType = AlterTableStatement.AlterType.AlterColumn;
      parser.SkipToken(true);
      parser.ExpectedExpression("COLUMN");
      parser.SkipToken(true);
      int rowNo = parser.TokenValue.RowNo;
      int colNo = parser.TokenValue.ColNo;
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
      this.alterColumn = new BaseCreateTableStatement.ColumnDescr((BaseCreateTableStatement) this, rowNo, colNo, columnName, dataType, width, codePage, allowNull, readOnly, encrypted, packed, defaultValue, setIdentity, identitySeed, identityStep, caption, description);
    }

    private void ParseAddColumnOrConstraint(SQLParser parser)
    {
      this.alterType = AlterTableStatement.AlterType.AddColumnOrConstraint;
      this.ParseColumns(parser);
    }

    private void ParseDropColumnOrConstraint(SQLParser parser)
    {
      this.alterType = AlterTableStatement.AlterType.DropColumnOrConstraint;
      this.dropItems = new List<AlterTableStatement.DropItem>();
      do
      {
        parser.SkipToken(true);
        if (parser.IsToken("COLUMN"))
        {
          parser.SkipToken(true);
          this.dropItems.Add(new AlterTableStatement.DropItem(parser.TokenValue.Token, parser.TokenValue.RowNo, parser.TokenValue.ColNo, AlterTableStatement.DropItemType.Column));
        }
        else
        {
          if (parser.IsToken("CONSTRAINT") && parser.TokenValue.TokenType == TokenType.Unknown)
            parser.SkipToken(true);
          this.dropItems.Add(new AlterTableStatement.DropItem(parser.TokenValue.Token, parser.TokenValue.RowNo, parser.TokenValue.ColNo, AlterTableStatement.DropItemType.Constraint));
        }
        parser.SkipToken(false);
      }
      while (parser.IsToken(","));
    }

    protected override IQueryResult OnExecuteQuery()
    {
      base.OnExecuteQuery();
      List<string> foreignKeys = (List<string>) null;
      using (IVistaDBTableSchema vistaDbTableSchema = this.Database.TableSchema(this.tableName))
      {
        switch (this.alterType)
        {
          case AlterTableStatement.AlterType.NewDescription:
            vistaDbTableSchema.Description = this.tableDescription;
            break;
          case AlterTableStatement.AlterType.AlterColumn:
            this.AlterColumn(vistaDbTableSchema);
            break;
          case AlterTableStatement.AlterType.AddColumnOrConstraint:
            this.AddColumns(vistaDbTableSchema);
            this.AddConstraints(vistaDbTableSchema);
            break;
          case AlterTableStatement.AlterType.DropColumnOrConstraint:
            foreignKeys = this.DropColumnOrConstraint(vistaDbTableSchema);
            break;
          case AlterTableStatement.AlterType.SyncService:
            if (this.activateSyncService)
              this.Database.ActivateSyncService(this.tableName);
            else
              this.Database.DeactivateSyncService(this.tableName);
            return (IQueryResult) null;
        }
        this.Database.AlterTable(this.tableName, vistaDbTableSchema);
      }
      if (this.alterType == AlterTableStatement.AlterType.AddColumnOrConstraint)
      {
        if (this.foreignKeys.Count > 0)
        {
          using (IVistaDBTable table = this.Database.OpenTable(this.tableName, false, false))
            this.AddForeignKeys(table);
        }
      }
      else
        this.DropForeignKeys(foreignKeys);
      return (IQueryResult) null;
    }

    private void AlterColumn(IVistaDBTableSchema tableSchema)
    {
      if (Utils.IsCharacterDataType(this.alterColumn.DataType))
        tableSchema.AlterColumnType(this.alterColumn.ColumnName, this.alterColumn.DataType, this.alterColumn.Width, this.alterColumn.CodePage);
      else
        tableSchema.AlterColumnType(this.alterColumn.ColumnName, this.alterColumn.DataType);
      tableSchema.DefineColumnAttributes(this.alterColumn.ColumnName, this.alterColumn.AllowNull, this.alterColumn.ReadOnly, this.alterColumn.Encrypted, this.alterColumn.Packed, this.alterColumn.Caption, this.alterColumn.Description);
      if (this.alterColumn.SetIdentity)
      {
        tableSchema.DefineIdentity(this.alterColumn.ColumnName, this.alterColumn.IdentitySeed, this.alterColumn.IdentityStep);
      }
      else
      {
        if (tableSchema.Identities[this.alterColumn.ColumnName] != null)
          tableSchema.DropIdentity(this.alterColumn.ColumnName);
        if (this.alterColumn.DefaultValue != null)
        {
          tableSchema.DefineDefaultValue(this.alterColumn.ColumnName, this.alterColumn.DefaultValue, this.alterColumn.DefaultValueUseInUpdate, this.alterColumn.DefaultValueDescription);
        }
        else
        {
          if (tableSchema.DefaultValues[this.alterColumn.ColumnName] == null)
            return;
          tableSchema.DropDefaultValue(this.alterColumn.ColumnName);
        }
      }
    }

    private List<string> DropColumnOrConstraint(IVistaDBTableSchema tableSchema)
    {
      List<string> stringList = new List<string>();
      foreach (AlterTableStatement.DropItem dropItem in this.dropItems)
      {
        switch (dropItem.Type)
        {
          case AlterTableStatement.DropItemType.Column:
            tableSchema.DropColumn(dropItem.Name);
            continue;
          case AlterTableStatement.DropItemType.Constraint:
            if (tableSchema.Constraints[dropItem.Name] != null)
            {
              tableSchema.DropConstraint(dropItem.Name);
              continue;
            }
            if (!tableSchema.ForeignKeys.ContainsKey(dropItem.Name))
              throw new VistaDBSQLException(622, dropItem.Name, dropItem.LineNo, dropItem.SymbolNo);
            stringList.Add(dropItem.Name);
            continue;
          default:
            continue;
        }
      }
      return stringList;
    }

    private void DropForeignKeys(List<string> foreignKeys)
    {
      if (foreignKeys == null || foreignKeys.Count == 0)
        return;
      using (IVistaDBTable vistaDbTable = this.Database.OpenTable(this.tableName, false, false))
      {
        foreach (string foreignKey in foreignKeys)
          vistaDbTable.DropForeignKey(foreignKey);
      }
    }

    private class DropItem
    {
      private string name;
      private int lineNo;
      private int symbolNo;
      private AlterTableStatement.DropItemType type;

      public DropItem(string name, int lineNo, int symbolNo, AlterTableStatement.DropItemType type)
      {
        this.name = name;
        this.lineNo = lineNo;
        this.symbolNo = symbolNo;
        this.type = type;
      }

      public string Name
      {
        get
        {
          return this.name;
        }
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

      public AlterTableStatement.DropItemType Type
      {
        get
        {
          return this.type;
        }
      }
    }

    private enum DropItemType
    {
      Column,
      Constraint,
    }

    private enum AlterType
    {
      NewDescription,
      AlterColumn,
      AddColumnOrConstraint,
      DropColumnOrConstraint,
      SyncService,
    }
  }
}
