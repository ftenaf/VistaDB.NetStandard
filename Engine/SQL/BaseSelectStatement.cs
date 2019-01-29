using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal abstract class BaseSelectStatement : Statement
  {
    protected TableCollection sourceTables = new TableCollection();
    internal const string SCHEMA_COLUMNNAME = "ColumnName";
    internal const string SCHEMA_COLUMNORDINAL = "ColumnOrdinal";
    internal const string SCHEMA_COLUMNSIZE = "ColumnSize";
    internal const string SCHEMA_NUMERICPRECISION = "NumericPrecision";
    internal const string SCHEMA_NUMERICSCALE = "NumericScale";
    internal const string SCHEMA_ISUNIQUE = "IsUnique";
    internal const string SCHEMA_ISKEY = "IsKey";
    internal const string SCHEMA_BASESERVERNAME = "BaseServerName";
    internal const string SCHEMA_BASECATALOGNAME = "BaseCatalogName";
    internal const string SCHEMA_BASECOLUMNNAME = "BaseColumnName";
    internal const string SCHEMA_BASESCHEMANAME = "BaseSchemaName";
    internal const string SCHEMA_BASETABLENAME = "BaseTableName";
    internal const string SCHEMA_DATATYPE = "DataType";
    internal const string SCHEMA_ALLOWDBNULL = "AllowDBNull";
    internal const string SCHEMA_PROVIDERTYPE = "ProviderType";
    internal const string SCHEMA_ISALIASED = "IsAliased";
    internal const string SCHEMA_ISEXPRESSION = "IsExpression";
    internal const string SCHEMA_ISIDENTITY = "IsIdentity";
    internal const string SCHEMA_ISAUTOINCREMENT = "IsAutoIncrement";
    internal const string SCHEMA_ISROWVERSION = "IsRowVersion";
    internal const string SCHEMA_ISHIDDEN = "IsHidden";
    internal const string SCHEMA_ISLONG = "IsLong";
    internal const string SCHEMA_ISREADONLY = "IsReadOnly";
    internal const string SCHEMA_PROVIDERDATATYPE = "ProviderSpecificDataType";
    internal const string SCHEMA_DEFAULTVALUE = "DefaultValue";
    internal const string SCHEMA_DATATYPENAME = "DataTypeName";
    protected ConstraintOperations constraintOperations;
    protected BaseSelectStatement.WhereClause whereClause;
    protected IRowSet join;

    protected BaseSelectStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void DoBeforeParse()
    {
      this.whereClause = new BaseSelectStatement.WhereClause(this);
    }

    protected bool ParseFromClause(SQLParser parser)
    {
      if (!parser.IsToken("FROM"))
        return false;
      parser.SkipToken(true);
      int num1 = 0;
      while (true)
      {
        IRowSet table;
        int num2 = num1 + this.ParseTable(out table, parser);
        this.join = this.join == null ? table : (IRowSet) new CrossJoin(this.join, table);
        num1 = num2 + this.ParseJoins(this.join, out this.join, parser);
        if (parser.IsToken(","))
          parser.SkipToken(true);
        else
          break;
      }
      this.sourceTables.Capacity = num1;
      return true;
    }

    protected int ParseTable(out IRowSet table, SQLParser parser)
    {
      ITableValuedFunction func = (ITableValuedFunction) null;
      int rowNo = parser.TokenValue.RowNo;
      int colNo = parser.TokenValue.ColNo;
      int symbolNo = parser.TokenValue.SymbolNo;
      SelectStatement statement;
      string str;
      string alias;
      if (parser.IsToken("("))
      {
        parser.SkipToken(true);
        if (!parser.IsToken("SELECT") || parser.TokenValue.TokenType != TokenType.Unknown)
        {
          int joins = this.ParseJoins((IRowSet) null, out table, parser);
          parser.ExpectedExpression(")");
          parser.SkipToken(false);
          return joins;
        }
        Statement parent = parser.Parent;
        statement = new SelectStatement(parser.Parent.Connection, (Statement) this, parser, 0L);
        parser.Parent = parent;
        str = (string) null;
        alias = (string) null;
        parser.ExpectedExpression(")");
        parser.SkipToken(false);
      }
      else
      {
        str = parser.GetTableName((Statement) this);
        alias = str;
        statement = (SelectStatement) null;
        if (parser.SkipToken(false) && parser.IsToken("("))
        {
          func = parser.CreateSpecialFunction(str, rowNo, colNo, symbolNo);
          parser.SkipToken(false);
        }
      }
      if (!parser.EndOfText)
      {
        if (parser.IsToken("AS"))
        {
          parser.SkipToken(true);
          alias = parser.TokenValue.Token;
          parser.SkipToken(false);
        }
        else if (!parser.IsToken("INNER") && !parser.IsToken("JOIN") && (!parser.IsToken("LEFT") && !parser.IsToken("RIGHT")) && (!parser.IsToken("CROSS") && !parser.IsToken("WHERE") && (!parser.IsToken("HAVING") && !parser.IsToken("GROUP"))) && (!parser.IsToken("ORDER") && !parser.IsToken("UNION") && (!parser.IsToken("ON") && !parser.IsToken(")")) && (!parser.IsToken(";") && !parser.IsToken(",") && (!parser.IsToken("END") && !parser.IsToken("ELSE")))))
        {
          alias = parser.TokenValue.Token;
          parser.SkipToken(false);
        }
      }
      table = statement != null ? (IRowSet) new QuerySourceTable((Statement) this, statement, alias, 0, rowNo, colNo) : (func == null ? (IRowSet) new NativeSourceTable((Statement) this, str, alias, 0, rowNo, colNo) : (IRowSet) new FuncSourceTable((Statement) this, func, alias, 0, rowNo, colNo));
      return 1;
    }

    protected int ParseJoins(IRowSet leftRowSet, out IRowSet rowSet, SQLParser parser)
    {
      int num = 0;
      if (leftRowSet == null)
      {
        num += this.ParseTable(out leftRowSet, parser);
        rowSet = leftRowSet;
      }
      else
        rowSet = (IRowSet) null;
      while (true)
      {
        if (parser.IsToken("INNER"))
        {
          parser.SkipToken(true);
          parser.ExpectedExpression("JOIN");
          num += this.ParseInnerJoin(leftRowSet, out rowSet, parser);
        }
        else if (parser.IsToken("JOIN"))
          num += this.ParseInnerJoin(leftRowSet, out rowSet, parser);
        else if (parser.IsToken("LEFT"))
          num += this.ParseLeftJoin(leftRowSet, out rowSet, parser);
        else if (parser.IsToken("RIGHT"))
          num += this.ParseRightJoin(leftRowSet, out rowSet, parser);
        else if (parser.IsToken("CROSS"))
          num += this.ParseCrossJoin(leftRowSet, out rowSet, parser);
        else
          break;
        leftRowSet = rowSet;
      }
      if (rowSet == null)
        rowSet = leftRowSet;
      return num;
    }

    private int ParseInnerJoin(IRowSet leftRowSet, out IRowSet rowSet, SQLParser parser)
    {
      parser.SkipToken(true);
      IRowSet rowSet1;
      int joins = this.ParseJoins((IRowSet) null, out rowSet1, parser);
      parser.ExpectedExpression("ON");
      Signature signature = parser.NextSignature(true, true, 6);
      rowSet = (IRowSet) new InnerJoin(signature, leftRowSet, rowSet1);
      return joins;
    }

    private int ParseLeftJoin(IRowSet leftRowSet, out IRowSet rowSet, SQLParser parser)
    {
      parser.SkipToken(true);
      if (parser.IsToken("OUTER"))
        parser.SkipToken(true);
      parser.ExpectedExpression("JOIN");
      parser.SkipToken(true);
      IRowSet rowSet1;
      int joins = this.ParseJoins((IRowSet) null, out rowSet1, parser);
      parser.ExpectedExpression("ON");
      Signature signature = parser.NextSignature(true, true, 6);
      rowSet = (IRowSet) new LeftJoin(signature, leftRowSet, rowSet1);
      return joins;
    }

    private int ParseRightJoin(IRowSet leftRowSet, out IRowSet rowSet, SQLParser parser)
    {
      parser.SkipToken(true);
      if (parser.IsToken("OUTER"))
        parser.SkipToken(true);
      parser.ExpectedExpression("JOIN");
      parser.SkipToken(true);
      IRowSet rowSet1;
      int joins = this.ParseJoins((IRowSet) null, out rowSet1, parser);
      parser.ExpectedExpression("ON");
      Signature signature = parser.NextSignature(true, true, 6);
      rowSet = (IRowSet) new LeftJoin(signature, rowSet1, leftRowSet);
      return joins;
    }

    private int ParseCrossJoin(IRowSet leftRowSet, out IRowSet rowSet, SQLParser parser)
    {
      parser.SkipToken(true);
      parser.ExpectedExpression("JOIN");
      parser.SkipToken(true);
      IRowSet table1;
      int table2 = this.ParseTable(out table1, parser);
      rowSet = (IRowSet) new CrossJoin(leftRowSet, table1);
      return table2;
    }

    protected void ParseWhereClause(SQLParser parser)
    {
      if (!parser.IsToken("WHERE"))
        return;
      this.whereClause.Signature = parser.NextSignature(true, true, 6);
    }

    public override SearchColumnResult GetTableByColumnName(string columnName, out SourceTable table, out int columnIndex)
    {
      table = (SourceTable) null;
      columnIndex = -1;
      foreach (SourceTable sourceTable in (List<SourceTable>) this.sourceTables)
      {
        int columnOrdinal = sourceTable.Schema.GetColumnOrdinal(columnName);
        if (columnOrdinal >= 0)
        {
          if (table != null)
            return SearchColumnResult.Duplicated;
          columnIndex = columnOrdinal;
          table = sourceTable;
        }
      }
      return table == null ? SearchColumnResult.NotFound : SearchColumnResult.Found;
    }

    public override SourceTable GetTableByAlias(string tableAlias)
    {
      foreach (SourceTable sourceTable in (List<SourceTable>) this.sourceTables)
      {
        if (this.connection.CompareString(sourceTable.Alias, tableAlias, true) == 0)
          return sourceTable;
      }
      if (this.parent == null)
        return (SourceTable) null;
      return this.parent.GetTableByAlias(tableAlias);
    }

    public override SourceTable GetSourceTable(int index)
    {
      return this.sourceTables[index];
    }

    protected abstract bool AcceptRow();

    public abstract void SetChanged();

    protected bool AcceptJoinedRow()
    {
      while (this.join.ExecuteRowset(this.constraintOperations))
      {
        if (this.whereClause.Execute(this.join.OuterRow))
          return true;
        this.join.Next(this.constraintOperations);
      }
      return false;
    }

    private void AcceptJoinedRowset()
    {
      while (this.join.ExecuteRowset(this.constraintOperations) && (!this.whereClause.Execute(this.join.OuterRow) || this.AcceptRow()))
        this.join.Next(this.constraintOperations);
    }

    protected void ExecuteJoin()
    {
      this.sourceTables.Open();
      this.sourceTables[0].First(this.constraintOperations);
      this.AcceptJoinedRowset();
    }

    protected void PrepareOptimize()
    {
      if (this.sourceTables.Count > 0 && this.connection.GetOptimization())
        this.constraintOperations = new ConstraintOperations(this.Database, this.sourceTables);
      else
        this.constraintOperations = (ConstraintOperations) null;
    }

    internal void Optimize()
    {
      this.SetChanged();
      if (this.constraintOperations == null)
        return;
      this.constraintOperations.ClearConstraints();
      if (this.whereClause.IsAlwaysFalse)
        return;
      int oldCount;
      if (!this.join.Optimize(this.constraintOperations))
      {
        if (this.constraintOperations.Count > 0)
          this.constraintOperations.ClearConstraints();
        oldCount = 0;
      }
      else
        oldCount = this.constraintOperations.Count;
      bool flag = !this.whereClause.Optimize(this.constraintOperations);
      if (flag)
        this.constraintOperations.RollBackAddedConstraints(oldCount);
      int count = this.constraintOperations.Count;
      if (oldCount < count && oldCount > 0)
        this.constraintOperations.AddLogicalAnd();
      if (!this.constraintOperations.AnalyzeOptimizationLevel())
      {
        this.constraintOperations = (ConstraintOperations) null;
      }
      else
      {
        if (!flag)
          return;
        this.constraintOperations.ResetFullOptimizationLevel();
      }
    }

    internal override ConstraintOperations ConstraintOperations
    {
      get
      {
        return this.constraintOperations;
      }
    }

    public override int SourceTableCount
    {
      get
      {
        return this.sourceTables.Count;
      }
    }

    internal OptimizationLevel OptimizationLevel
    {
      get
      {
        if (this.constraintOperations != null)
          return this.constraintOperations.OptimizationLevel;
        return OptimizationLevel.None;
      }
    }

    internal IRowSet RowSet
    {
      get
      {
        return this.join;
      }
    }

    internal static DataTable GetSchemaTableInstance()
    {
      return new DataTable("SchemaTable") { Locale = CultureInfo.InvariantCulture, Columns = { { "ColumnName", typeof (string) }, { "ColumnOrdinal", typeof (int) }, { "ColumnSize", typeof (int) }, { "NumericPrecision", typeof (short) }, { "NumericScale", typeof (short) }, { "IsUnique", typeof (bool) }, { "IsKey", typeof (bool) }, { "BaseServerName", typeof (string) }, { "BaseCatalogName", typeof (string) }, { "BaseColumnName", typeof (string) }, { "BaseSchemaName", typeof (string) }, { "BaseTableName", typeof (string) }, { "DataType", typeof (Type) }, { "AllowDBNull", typeof (bool) }, { "ProviderType", typeof (int) }, { "IsAliased", typeof (bool) }, { "IsExpression", typeof (bool) }, { "IsIdentity", typeof (bool) }, { "IsAutoIncrement", typeof (bool) }, { "IsRowVersion", typeof (bool) }, { "IsHidden", typeof (bool) }, { "IsLong", typeof (bool) }, { "IsReadOnly", typeof (bool) }, { "ProviderSpecificDataType", typeof (Type) }, { "DefaultValue", typeof (object) }, { "DataTypeName", typeof (string) } } };
    }

    protected class WhereClause
    {
      private BaseSelectStatement parent;
      private Signature signature;

      public WhereClause(BaseSelectStatement parent)
      {
        this.signature = (Signature) null;
        this.parent = parent;
      }

      private bool ResultValue()
      {
        IVistaDBValue vistaDbValue = (IVistaDBValue) this.signature.Execute();
        if (vistaDbValue != null && !vistaDbValue.IsNull && vistaDbValue.Type == VistaDBType.Bit)
          return (bool) vistaDbValue.Value;
        return false;
      }

      internal bool Execute(bool forceToCheck)
      {
        if ((forceToCheck || this.parent.OptimizationLevel != OptimizationLevel.Full) && !(this.signature == (Signature) null))
          return this.ResultValue();
        return true;
      }

      public void Prepare()
      {
        if (this.signature == (Signature) null)
          return;
        if (this.signature.Prepare() == SignatureType.Constant && this.signature.SignatureType != SignatureType.Constant)
          this.signature = (Signature) ConstantSignature.CreateSignature(this.signature.Execute(), (Statement) this.parent);
        if (this.signature.DataType != VistaDBType.Bit)
          throw new VistaDBSQLException(564, "", this.signature.LineNo, this.signature.SymbolNo);
      }

      public bool Optimize(ConstraintOperations constraints)
      {
        if (!(this.signature == (Signature) null) && this.signature.SignatureType != SignatureType.Constant)
          return this.signature.Optimize(constraints);
        return true;
      }

      public bool IsEquals(BaseSelectStatement.WhereClause clause)
      {
        return this.signature == clause.signature;
      }

      public void SetChanged()
      {
        if (!(this.signature != (Signature) null))
          return;
        this.signature.SetChanged();
      }

      public void ClearChanged()
      {
        if (!(this.signature != (Signature) null))
          return;
        this.signature.ClearChanged();
      }

      public bool IsAlwaysFalse
      {
        get
        {
          if (this.signature != (Signature) null && this.signature.SignatureType == SignatureType.Constant)
            return !this.Execute(false);
          return false;
        }
      }

      public bool IsAlwaysTrue
      {
        get
        {
          if (this.signature == (Signature) null)
            return true;
          if (this.signature.SignatureType == SignatureType.Constant)
            return this.Execute(false);
          return false;
        }
      }

      public Signature Signature
      {
        get
        {
          return this.signature;
        }
        set
        {
          bool distinct;
          if (value != (Signature) null && value.HasAggregateFunction(out distinct))
            throw new VistaDBSQLException(565, "", value.LineNo, value.SymbolNo);
          this.signature = value;
        }
      }

      public bool GetIsChanged()
      {
        if (this.signature != (Signature) null)
          return this.signature.GetIsChanged();
        return false;
      }
    }
  }
}
