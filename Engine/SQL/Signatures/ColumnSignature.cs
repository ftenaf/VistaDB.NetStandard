using System;
using System.Collections.Generic;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class ColumnSignature : Signature
  {
    private SourceTable table;
    private int columnIndex;
    private string columnName;
    private string tableAlias;
    private int width;
    private bool isKey;
    private bool isExpression;
    private bool isAutoIncrement;
    private bool isReadOnly;
    private string description;
    private string caption;
    private bool encrypted;
    private int codePage;
    private string identity;
    private string identityStep;
    private string identitySeed;
    private string defaultValue;
    private bool useInUpdate;
    private long tableVersion;

    private ColumnSignature(string tableAlias, string columnName, SQLParser parser)
      : base(parser)
    {
      this.tableAlias = tableAlias;
      this.columnName = columnName;
      this.dataType = VistaDBType.Unknown;
      this.table = (SourceTable) null;
      this.columnIndex = -1;
      this.tableVersion = -1L;
      this.width = 0;
      this.isKey = false;
      this.isAllowNull = false;
      this.isExpression = false;
      this.isAutoIncrement = false;
      this.isReadOnly = false;
      this.description = (string) null;
      this.caption = (string) null;
      this.encrypted = false;
      this.codePage = 0;
      this.identity = (string) null;
      this.identityStep = (string) null;
      this.identitySeed = (string) null;
      this.defaultValue = (string) null;
      this.useInUpdate = false;
      if (string.Compare(this.columnName, "*", StringComparison.OrdinalIgnoreCase) == 0)
        this.signatureType = SignatureType.MultiplyColumn;
      else
        this.signatureType = SignatureType.Column;
    }

    internal ColumnSignature(SourceTable table, int columnIndex, Statement parent)
      : base(parent)
    {
      IQuerySchemaInfo schema = table.Schema;
      if (this.parent == table.Parent)
        this.signatureType = SignatureType.Column;
      else
        this.signatureType = SignatureType.ExternalColumn;
      this.tableAlias = table.Alias;
      this.columnName = schema.GetAliasName(columnIndex);
      this.dataType = schema.GetColumnVistaDBType(columnIndex);
      this.width = schema.GetWidth(columnIndex);
      this.table = table;
      this.columnIndex = columnIndex;
      this.tableVersion = -1L;
      this.optimizable = true;
      this.isKey = schema.GetIsKey(columnIndex);
      this.isAllowNull = schema.GetIsAllowNull(columnIndex);
      this.isExpression = schema.GetIsExpression(columnIndex);
      this.isAutoIncrement = schema.GetIsAutoIncrement(columnIndex);
      this.isReadOnly = schema.GetIsReadOnly(columnIndex);
      this.description = schema.GetColumnDescription(columnIndex);
      this.caption = schema.GetColumnCaption(columnIndex);
      this.encrypted = schema.GetIsEncrypted(columnIndex);
      this.codePage = schema.GetCodePage(columnIndex);
      this.identity = schema.GetIdentity(columnIndex, out this.identityStep, out this.identitySeed);
      this.defaultValue = schema.GetDefaultValue(columnIndex, out this.useInUpdate);
    }

    internal static ColumnSignature CreateSignature(SQLParser parser)
    {
      string objectName;
      return new ColumnSignature(parser.ParseComplexName(out objectName), objectName, parser);
    }

    internal static ColumnSignature CreateSignature(string tableName, string columnName, SQLParser parser)
    {
      return new ColumnSignature(tableName, columnName, parser);
    }

    public void ExtractColumns(List<ColumnSignature> columnSignatureList)
    {
      if (this.columnName != "*")
        return;
      if (this.tableAlias != null)
      {
        SourceTable tableByAlias = this.parent.GetTableByAlias(this.tableAlias);
        if (tableByAlias == null)
          throw new VistaDBSQLException(572, this.tableAlias, this.lineNo, this.symbolNo);
        this.AddColumnsFromTable(tableByAlias, columnSignatureList);
      }
      else
      {
        int sourceTableCount = this.parent.SourceTableCount;
        if (sourceTableCount == 0)
          throw new VistaDBSQLException(608, this.columnName, this.lineNo, this.symbolNo);
        for (int index = 0; index < sourceTableCount; ++index)
          this.AddColumnsFromTable(this.parent.GetSourceTable(index), columnSignatureList);
      }
    }

    private void AddColumnsFromTable(SourceTable table, List<ColumnSignature> columnSignatureList)
    {
      table.RegisterColumnSignature(-1);
      int columnIndex = 0;
      for (int columnCount = table.Schema.ColumnCount; columnIndex < columnCount; ++columnIndex)
        columnSignatureList.Add(new ColumnSignature(table, columnIndex, this.parent));
    }

    protected override IColumn InternalExecute()
    {
      if (this.GetIsChanged() && this.table.Opened)
      {
        ((IValue) this.result).Value = this.table.GetValue(this.columnIndex);
        this.tableVersion = this.table.Version;
      }
      return this.result;
    }

    protected override void OnSimpleExecute()
    {
      if (!this.GetIsChanged())
        return;
      ((IValue) this.result).Value = ((IValue) this.table.SimpleGetColumn(this.columnIndex)).Value;
      this.tableVersion = this.table.Version;
    }

    public override SignatureType OnPrepare()
    {
      if (this.signatureType == SignatureType.MultiplyColumn)
        return this.signatureType;
      if (this.tableAlias != null)
      {
        this.table = this.parent.GetTableByAlias(this.tableAlias);
        if (this.table == null)
          throw new VistaDBSQLException(572, this.tableAlias, this.lineNo, this.symbolNo);
        this.columnIndex = this.table.Schema.GetColumnOrdinal(this.columnName);
      }
      else if (this.parent.GetTableByColumnName(this.columnName, out this.table, out this.columnIndex) == SearchColumnResult.Duplicated)
        throw new VistaDBSQLException(579, this.columnName, this.lineNo, this.symbolNo);
      if (this.columnIndex < 0)
        throw new VistaDBSQLException(567, this.columnName, this.lineNo, this.symbolNo);
      this.table.RegisterColumnSignature(this.columnIndex);
      IQuerySchemaInfo schema = this.table.Schema;
      this.dataType = schema.GetColumnVistaDBType(this.columnIndex);
      this.width = schema.GetWidth(this.columnIndex);
      this.isKey = schema.GetIsKey(this.columnIndex);
      this.isAllowNull = schema.GetIsAllowNull(this.columnIndex);
      this.isExpression = schema.GetIsExpression(this.columnIndex);
      this.isAutoIncrement = schema.GetIsAutoIncrement(this.columnIndex);
      this.isReadOnly = schema.GetIsReadOnly(this.columnIndex);
      this.optimizable = true;
      if (this.parent != this.table.Parent)
        this.signatureType = SignatureType.ExternalColumn;
      return this.signatureType;
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      distinct = false;
      return false;
    }

    public override int GetWidth()
    {
      return this.width;
    }

    protected override bool IsEquals(Signature signature)
    {
      if (signature is ColumnSignature && this.parent.Connection.CompareString(this.columnName, ((ColumnSignature) signature).ColumnName, true) == 0)
        return this.parent.Connection.CompareString(this.tableAlias, ((ColumnSignature) signature).TableAlias, true) == 0;
      return false;
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
    }

    public override void SetChanged()
    {
      this.tableVersion = -1L;
    }

    public override void ClearChanged()
    {
      this.tableVersion = this.table.Version;
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
    }

    public override bool AlwaysNull
    {
      get
      {
        return false;
      }
    }

    protected override bool InternalGetIsChanged()
    {
      if (this.table != null)
        return this.tableVersion != this.table.Version;
      return true;
    }

    protected internal long TableVersion
    {
      get
      {
        if (!this.InternalGetIsChanged())
          return this.tableVersion;
        return -1;
      }
    }

    public override int ColumnCount
    {
      get
      {
        return 1;
      }
    }

    public string ColumnName
    {
      get
      {
        return this.columnName;
      }
    }

    public string TableAlias
    {
      get
      {
        return this.tableAlias;
      }
    }

    public int ColumnIndex
    {
      get
      {
        return this.columnIndex;
      }
    }

    public SourceTable Table
    {
      get
      {
        return this.table;
      }
    }

    public bool IsKey
    {
      get
      {
        return this.isKey;
      }
    }

    public bool IsExpression
    {
      get
      {
        return this.isExpression;
      }
    }

    public bool IsAutoIncrement
    {
      get
      {
        return this.isAutoIncrement;
      }
    }

    public bool IsReadOnly
    {
      get
      {
        return this.isReadOnly;
      }
    }

    public string Description
    {
      get
      {
        return this.description;
      }
    }

    public string Caption
    {
      get
      {
        return this.caption;
      }
    }

    public bool Encrypted
    {
      get
      {
        return this.encrypted;
      }
    }

    public int CodePage
    {
      get
      {
        return this.codePage;
      }
    }

    public string Identity
    {
      get
      {
        return this.identity;
      }
    }

    public string IdentityStep
    {
      get
      {
        return this.identityStep;
      }
    }

    public string IdentitySeed
    {
      get
      {
        return this.identitySeed;
      }
    }

    public string DefaultValue
    {
      get
      {
        return this.defaultValue;
      }
    }

    public bool UseInUpdate
    {
      get
      {
        return this.useInUpdate;
      }
    }
  }
}
