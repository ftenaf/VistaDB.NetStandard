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
      dataType = VistaDBType.Unknown;
      table = (SourceTable) null;
      columnIndex = -1;
      tableVersion = -1L;
      width = 0;
      isKey = false;
      isAllowNull = false;
      isExpression = false;
      isAutoIncrement = false;
      isReadOnly = false;
      description = (string) null;
      caption = (string) null;
      encrypted = false;
      codePage = 0;
      identity = (string) null;
      identityStep = (string) null;
      identitySeed = (string) null;
      defaultValue = (string) null;
      useInUpdate = false;
      if (string.Compare(this.columnName, "*", StringComparison.OrdinalIgnoreCase) == 0)
        signatureType = SignatureType.MultiplyColumn;
      else
        signatureType = SignatureType.Column;
    }

    internal ColumnSignature(SourceTable table, int columnIndex, Statement parent)
      : base(parent)
    {
      IQuerySchemaInfo schema = table.Schema;
      if (this.parent == table.Parent)
        signatureType = SignatureType.Column;
      else
        signatureType = SignatureType.ExternalColumn;
      tableAlias = table.Alias;
      columnName = schema.GetAliasName(columnIndex);
      dataType = schema.GetColumnVistaDBType(columnIndex);
      width = schema.GetWidth(columnIndex);
      this.table = table;
      this.columnIndex = columnIndex;
      tableVersion = -1L;
      optimizable = true;
      isKey = schema.GetIsKey(columnIndex);
      isAllowNull = schema.GetIsAllowNull(columnIndex);
      isExpression = schema.GetIsExpression(columnIndex);
      isAutoIncrement = schema.GetIsAutoIncrement(columnIndex);
      isReadOnly = schema.GetIsReadOnly(columnIndex);
      description = schema.GetColumnDescription(columnIndex);
      caption = schema.GetColumnCaption(columnIndex);
      encrypted = schema.GetIsEncrypted(columnIndex);
      codePage = schema.GetCodePage(columnIndex);
      identity = schema.GetIdentity(columnIndex, out identityStep, out identitySeed);
      defaultValue = schema.GetDefaultValue(columnIndex, out useInUpdate);
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
      if (columnName != "*")
        return;
      if (tableAlias != null)
      {
        SourceTable tableByAlias = parent.GetTableByAlias(tableAlias);
        if (tableByAlias == null)
          throw new VistaDBSQLException(572, tableAlias, lineNo, symbolNo);
        AddColumnsFromTable(tableByAlias, columnSignatureList);
      }
      else
      {
        int sourceTableCount = parent.SourceTableCount;
        if (sourceTableCount == 0)
          throw new VistaDBSQLException(608, columnName, lineNo, symbolNo);
        for (int index = 0; index < sourceTableCount; ++index)
          AddColumnsFromTable(parent.GetSourceTable(index), columnSignatureList);
      }
    }

    private void AddColumnsFromTable(SourceTable table, List<ColumnSignature> columnSignatureList)
    {
      table.RegisterColumnSignature(-1);
      int columnIndex = 0;
      for (int columnCount = table.Schema.ColumnCount; columnIndex < columnCount; ++columnIndex)
        columnSignatureList.Add(new ColumnSignature(table, columnIndex, parent));
    }

    protected override IColumn InternalExecute()
    {
      if (GetIsChanged() && table.Opened)
      {
        ((IValue) result).Value = table.GetValue(columnIndex);
        tableVersion = table.Version;
      }
      return result;
    }

    protected override void OnSimpleExecute()
    {
      if (!GetIsChanged())
        return;
      ((IValue) result).Value = ((IValue) table.SimpleGetColumn(columnIndex)).Value;
      tableVersion = table.Version;
    }

    public override SignatureType OnPrepare()
    {
      if (signatureType == SignatureType.MultiplyColumn)
        return signatureType;
      if (tableAlias != null)
      {
        table = parent.GetTableByAlias(tableAlias);
        if (table == null)
          throw new VistaDBSQLException(572, tableAlias, lineNo, symbolNo);
        columnIndex = table.Schema.GetColumnOrdinal(columnName);
      }
      else if (parent.GetTableByColumnName(columnName, out table, out columnIndex) == SearchColumnResult.Duplicated)
        throw new VistaDBSQLException(579, columnName, lineNo, symbolNo);
      if (columnIndex < 0)
        throw new VistaDBSQLException(567, columnName, lineNo, symbolNo);
      table.RegisterColumnSignature(columnIndex);
      IQuerySchemaInfo schema = table.Schema;
      dataType = schema.GetColumnVistaDBType(columnIndex);
      width = schema.GetWidth(columnIndex);
      isKey = schema.GetIsKey(columnIndex);
      isAllowNull = schema.GetIsAllowNull(columnIndex);
      isExpression = schema.GetIsExpression(columnIndex);
      isAutoIncrement = schema.GetIsAutoIncrement(columnIndex);
      isReadOnly = schema.GetIsReadOnly(columnIndex);
      optimizable = true;
      if (parent != table.Parent)
        signatureType = SignatureType.ExternalColumn;
      return signatureType;
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      distinct = false;
      return false;
    }

    public override int GetWidth()
    {
      return width;
    }

    protected override bool IsEquals(Signature signature)
    {
      if (signature is ColumnSignature && parent.Connection.CompareString(columnName, ((ColumnSignature) signature).ColumnName, true) == 0)
        return parent.Connection.CompareString(tableAlias, ((ColumnSignature) signature).TableAlias, true) == 0;
      return false;
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
    }

    public override void SetChanged()
    {
      tableVersion = -1L;
    }

    public override void ClearChanged()
    {
      tableVersion = table.Version;
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
      if (table != null)
        return tableVersion != table.Version;
      return true;
    }

    protected internal long TableVersion
    {
      get
      {
        if (!InternalGetIsChanged())
          return tableVersion;
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
        return columnName;
      }
    }

    public string TableAlias
    {
      get
      {
        return tableAlias;
      }
    }

    public int ColumnIndex
    {
      get
      {
        return columnIndex;
      }
    }

    public SourceTable Table
    {
      get
      {
        return table;
      }
    }

    public bool IsKey
    {
      get
      {
        return isKey;
      }
    }

    public bool IsExpression
    {
      get
      {
        return isExpression;
      }
    }

    public bool IsAutoIncrement
    {
      get
      {
        return isAutoIncrement;
      }
    }

    public bool IsReadOnly
    {
      get
      {
        return isReadOnly;
      }
    }

    public string Description
    {
      get
      {
        return description;
      }
    }

    public string Caption
    {
      get
      {
        return caption;
      }
    }

    public bool Encrypted
    {
      get
      {
        return encrypted;
      }
    }

    public int CodePage
    {
      get
      {
        return codePage;
      }
    }

    public string Identity
    {
      get
      {
        return identity;
      }
    }

    public string IdentityStep
    {
      get
      {
        return identityStep;
      }
    }

    public string IdentitySeed
    {
      get
      {
        return identitySeed;
      }
    }

    public string DefaultValue
    {
      get
      {
        return defaultValue;
      }
    }

    public bool UseInUpdate
    {
      get
      {
        return useInUpdate;
      }
    }
  }
}
