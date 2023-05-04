using System.Collections.Generic;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class AlterTableStatement : BaseCreateTableStatement
  {
    private AlterType alterType;
    private ColumnDescr alterColumn;
    private List<DropItem> dropItems;
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
          activateSyncService = true;
        }
        else
        {
          parser.ExpectedExpression("OFF");
          activateSyncService = false;
        }
        alterType = AlterType.SyncService;
        parser.SkipToken(false);
      }
      else if (parser.IsToken("DESCRIPTION"))
        ParseNewDescription(parser);
      else if (parser.IsToken("ALTER"))
        ParseAlterColumn(parser);
      else if (parser.IsToken("ADD"))
      {
        ParseAddColumnOrConstraint(parser);
      }
      else
      {
        if (!parser.IsToken("DROP"))
          throw new VistaDBSQLException(507, "DESCRIPTION, ALTER COLUMN, ADD, DROP, SYNCHRONIZATION", parser.TokenValue.RowNo, parser.TokenValue.ColNo);
        ParseDropColumnOrConstraint(parser);
      }
    }

    private void ParseNewDescription(SQLParser parser)
    {
      alterType = AlterType.NewDescription;
      parser.SkipToken(true);
      tableDescription = parser.TokenValue.Token;
      parser.SkipToken(false);
    }

    private void ParseAlterColumn(SQLParser parser)
    {
      alterType = AlterType.AlterColumn;
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
      ParseColumnAttributes(out columnName, out dataType, out width, out codePage, out allowNull, out readOnly, out encrypted, out packed, out defaultValue, out setIdentity, out identitySeed, out identityStep, out caption, out description, parser);
      alterColumn = new ColumnDescr((BaseCreateTableStatement) this, rowNo, colNo, columnName, dataType, width, codePage, allowNull, readOnly, encrypted, packed, defaultValue, setIdentity, identitySeed, identityStep, caption, description);
    }

    private void ParseAddColumnOrConstraint(SQLParser parser)
    {
      alterType = AlterType.AddColumnOrConstraint;
      ParseColumns(parser);
    }

    private void ParseDropColumnOrConstraint(SQLParser parser)
    {
      alterType = AlterType.DropColumnOrConstraint;
      dropItems = new List<DropItem>();
      do
      {
        parser.SkipToken(true);
        if (parser.IsToken("COLUMN"))
        {
          parser.SkipToken(true);
          dropItems.Add(new DropItem(parser.TokenValue.Token, parser.TokenValue.RowNo, parser.TokenValue.ColNo, DropItemType.Column));
        }
        else
        {
          if (parser.IsToken("CONSTRAINT") && parser.TokenValue.TokenType == TokenType.Unknown)
            parser.SkipToken(true);
          dropItems.Add(new DropItem(parser.TokenValue.Token, parser.TokenValue.RowNo, parser.TokenValue.ColNo, DropItemType.Constraint));
        }
        parser.SkipToken(false);
      }
      while (parser.IsToken(","));
    }

    protected override IQueryResult OnExecuteQuery()
    {
      base.OnExecuteQuery();
      List<string> foreignKeys = (List<string>) null;
      using (IVistaDBTableSchema vistaDbTableSchema = Database.TableSchema(tableName))
      {
        switch (alterType)
        {
          case AlterType.NewDescription:
            vistaDbTableSchema.Description = tableDescription;
            break;
          case AlterType.AlterColumn:
            AlterColumn(vistaDbTableSchema);
            break;
          case AlterType.AddColumnOrConstraint:
            AddColumns(vistaDbTableSchema);
            AddConstraints(vistaDbTableSchema);
            break;
          case AlterType.DropColumnOrConstraint:
            foreignKeys = DropColumnOrConstraint(vistaDbTableSchema);
            break;
          case AlterType.SyncService:
            if (activateSyncService)
              Database.ActivateSyncService(tableName);
            else
              Database.DeactivateSyncService(tableName);
            return (IQueryResult) null;
        }
        Database.AlterTable(tableName, vistaDbTableSchema);
      }
      if (alterType == AlterType.AddColumnOrConstraint)
      {
        if (this.foreignKeys.Count > 0)
        {
          using (IVistaDBTable table = Database.OpenTable(tableName, false, false))
            AddForeignKeys(table);
        }
      }
      else
        DropForeignKeys(foreignKeys);
      return (IQueryResult) null;
    }

    private void AlterColumn(IVistaDBTableSchema tableSchema)
    {
      if (Utils.IsCharacterDataType(alterColumn.DataType))
        tableSchema.AlterColumnType(alterColumn.ColumnName, alterColumn.DataType, alterColumn.Width, alterColumn.CodePage);
      else
        tableSchema.AlterColumnType(alterColumn.ColumnName, alterColumn.DataType);
      tableSchema.DefineColumnAttributes(alterColumn.ColumnName, alterColumn.AllowNull, alterColumn.ReadOnly, alterColumn.Encrypted, alterColumn.Packed, alterColumn.Caption, alterColumn.Description);
      if (alterColumn.SetIdentity)
      {
        tableSchema.DefineIdentity(alterColumn.ColumnName, alterColumn.IdentitySeed, alterColumn.IdentityStep);
      }
      else
      {
        if (tableSchema.Identities[alterColumn.ColumnName] != null)
          tableSchema.DropIdentity(alterColumn.ColumnName);
        if (alterColumn.DefaultValue != null)
        {
          tableSchema.DefineDefaultValue(alterColumn.ColumnName, alterColumn.DefaultValue, alterColumn.DefaultValueUseInUpdate, alterColumn.DefaultValueDescription);
        }
        else
        {
          if (tableSchema.DefaultValues[alterColumn.ColumnName] == null)
            return;
          tableSchema.DropDefaultValue(alterColumn.ColumnName);
        }
      }
    }

    private List<string> DropColumnOrConstraint(IVistaDBTableSchema tableSchema)
    {
      List<string> stringList = new List<string>();
      foreach (DropItem dropItem in dropItems)
      {
        switch (dropItem.Type)
        {
          case DropItemType.Column:
            tableSchema.DropColumn(dropItem.Name);
            continue;
          case DropItemType.Constraint:
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
      using (IVistaDBTable vistaDbTable = Database.OpenTable(tableName, false, false))
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
      private DropItemType type;

      public DropItem(string name, int lineNo, int symbolNo, DropItemType type)
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
          return name;
        }
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

      public DropItemType Type
      {
        get
        {
          return type;
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
