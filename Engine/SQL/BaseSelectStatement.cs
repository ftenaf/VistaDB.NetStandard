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
    protected WhereClause whereClause;
    protected IRowSet join;

    protected BaseSelectStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void DoBeforeParse()
    {
      whereClause = new WhereClause(this);
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
        int num2 = num1 + ParseTable(out table, parser);
        join = join == null ? table : (IRowSet) new CrossJoin(join, table);
        num1 = num2 + ParseJoins(join, out join, parser);
        if (parser.IsToken(","))
          parser.SkipToken(true);
        else
          break;
      }
      sourceTables.Capacity = num1;
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
          int joins = ParseJoins((IRowSet) null, out table, parser);
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
        num += ParseTable(out leftRowSet, parser);
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
          num += ParseInnerJoin(leftRowSet, out rowSet, parser);
        }
        else if (parser.IsToken("JOIN"))
          num += ParseInnerJoin(leftRowSet, out rowSet, parser);
        else if (parser.IsToken("LEFT"))
          num += ParseLeftJoin(leftRowSet, out rowSet, parser);
        else if (parser.IsToken("RIGHT"))
          num += ParseRightJoin(leftRowSet, out rowSet, parser);
        else if (parser.IsToken("CROSS"))
          num += ParseCrossJoin(leftRowSet, out rowSet, parser);
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
      int joins = ParseJoins((IRowSet) null, out rowSet1, parser);
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
      int joins = ParseJoins((IRowSet) null, out rowSet1, parser);
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
      int joins = ParseJoins((IRowSet) null, out rowSet1, parser);
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
      int table2 = ParseTable(out table1, parser);
      rowSet = (IRowSet) new CrossJoin(leftRowSet, table1);
      return table2;
    }

    protected void ParseWhereClause(SQLParser parser)
    {
      if (!parser.IsToken("WHERE"))
        return;
      whereClause.Signature = parser.NextSignature(true, true, 6);
    }

    public override SearchColumnResult GetTableByColumnName(string columnName, out SourceTable table, out int columnIndex)
    {
      table = (SourceTable) null;
      columnIndex = -1;
      foreach (SourceTable sourceTable in (List<SourceTable>) sourceTables)
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
      foreach (SourceTable sourceTable in (List<SourceTable>) sourceTables)
      {
        if (connection.CompareString(sourceTable.Alias, tableAlias, true) == 0)
          return sourceTable;
      }
      if (parent == null)
        return (SourceTable) null;
      return parent.GetTableByAlias(tableAlias);
    }

    public override SourceTable GetSourceTable(int index)
    {
      return sourceTables[index];
    }

    protected abstract bool AcceptRow();

    public abstract void SetChanged();

    protected bool AcceptJoinedRow()
    {
      while (join.ExecuteRowset(constraintOperations))
      {
        if (whereClause.Execute(join.OuterRow))
          return true;
        join.Next(constraintOperations);
      }
      return false;
    }

    private void AcceptJoinedRowset()
    {
      while (join.ExecuteRowset(constraintOperations) && (!whereClause.Execute(join.OuterRow) || AcceptRow()))
        join.Next(constraintOperations);
    }

    protected void ExecuteJoin()
    {
      sourceTables.Open();
      sourceTables[0].First(constraintOperations);
      AcceptJoinedRowset();
    }

    protected void PrepareOptimize()
    {
      if (sourceTables.Count > 0 && connection.GetOptimization())
        constraintOperations = new ConstraintOperations(Database, sourceTables);
      else
        constraintOperations = (ConstraintOperations) null;
    }

    internal void Optimize()
    {
      SetChanged();
      if (constraintOperations == null)
        return;
      constraintOperations.ClearConstraints();
      if (whereClause.IsAlwaysFalse)
        return;
      int oldCount;
      if (!join.Optimize(constraintOperations))
      {
        if (constraintOperations.Count > 0)
          constraintOperations.ClearConstraints();
        oldCount = 0;
      }
      else
        oldCount = constraintOperations.Count;
      bool flag = !whereClause.Optimize(constraintOperations);
      if (flag)
        constraintOperations.RollBackAddedConstraints(oldCount);
      int count = constraintOperations.Count;
      if (oldCount < count && oldCount > 0)
        constraintOperations.AddLogicalAnd();
      if (!constraintOperations.AnalyzeOptimizationLevel())
      {
        constraintOperations = (ConstraintOperations) null;
      }
      else
      {
        if (!flag)
          return;
        constraintOperations.ResetFullOptimizationLevel();
      }
    }

    internal override ConstraintOperations ConstraintOperations
    {
      get
      {
        return constraintOperations;
      }
    }

    public override int SourceTableCount
    {
      get
      {
        return sourceTables.Count;
      }
    }

    internal OptimizationLevel OptimizationLevel
    {
      get
      {
        if (constraintOperations != null)
          return constraintOperations.OptimizationLevel;
        return OptimizationLevel.None;
      }
    }

    internal IRowSet RowSet
    {
      get
      {
        return join;
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
        signature = (Signature) null;
        this.parent = parent;
      }

      private bool ResultValue()
      {
        IVistaDBValue vistaDbValue = (IVistaDBValue) signature.Execute();
        if (vistaDbValue != null && !vistaDbValue.IsNull && vistaDbValue.Type == VistaDBType.Bit)
          return (bool) vistaDbValue.Value;
        return false;
      }

      internal bool Execute(bool forceToCheck)
      {
        if ((forceToCheck || parent.OptimizationLevel != OptimizationLevel.Full) && !(signature == (Signature) null))
          return ResultValue();
        return true;
      }

      public void Prepare()
      {
        if (signature == (Signature) null)
          return;
        if (signature.Prepare() == SignatureType.Constant && signature.SignatureType != SignatureType.Constant)
          signature = (Signature) ConstantSignature.CreateSignature(signature.Execute(), (Statement) parent);
        if (signature.DataType != VistaDBType.Bit)
          throw new VistaDBSQLException(564, "", signature.LineNo, signature.SymbolNo);
      }

      public bool Optimize(ConstraintOperations constraints)
      {
        if (!(signature == (Signature) null) && signature.SignatureType != SignatureType.Constant)
          return signature.Optimize(constraints);
        return true;
      }

      public bool IsEquals(WhereClause clause)
      {
        return signature == clause.signature;
      }

      public void SetChanged()
      {
        if (!(signature != (Signature) null))
          return;
        signature.SetChanged();
      }

      public void ClearChanged()
      {
        if (!(signature != (Signature) null))
          return;
        signature.ClearChanged();
      }

      public bool IsAlwaysFalse
      {
        get
        {
          if (signature != (Signature) null && signature.SignatureType == SignatureType.Constant)
            return !Execute(false);
          return false;
        }
      }

      public bool IsAlwaysTrue
      {
        get
        {
          if (signature == (Signature) null)
            return true;
          if (signature.SignatureType == SignatureType.Constant)
            return Execute(false);
          return false;
        }
      }

      public Signature Signature
      {
        get
        {
          return signature;
        }
        set
        {
          bool distinct;
          if (value != (Signature) null && value.HasAggregateFunction(out distinct))
            throw new VistaDBSQLException(565, "", value.LineNo, value.SymbolNo);
          signature = value;
        }
      }

      public bool GetIsChanged()
      {
        if (signature != (Signature) null)
          return signature.GetIsChanged();
        return false;
      }
    }
  }
}
