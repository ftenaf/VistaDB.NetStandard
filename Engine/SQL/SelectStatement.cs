using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal class SelectStatement : BaseSelectStatement, IQuerySchemaInfo, IQueryResult
  {
    private long topCount = long.MaxValue;
    private bool endOfTable = true;
    protected SelectStatement.AddRowMethod addRowMethod;
    private bool distinct;
    private TempTable resultTable;
    private SelectStatement.ResultColumnList resultColumns;
    private SelectStatement.GroupColumnList groupColumns;
    private SelectStatement.HavingClause havingClause;
    private SelectStatement.OrderColumnList orderColumns;
    private Signature topCountSig;
    private bool hasAggregate;
    private bool ordered;
    private bool unionAll;
    private SelectStatement unionQuery;
    private bool simpleAggregateAdded;
    private SelectStatement.AggregateExpressionList aggregateExpressions;
    private HashGroupTable hashGroupTable;
    private TempTable groupTable;
    private bool distinctAggregate;
    private SelectStatement.ResultColumnList nonAggColumns;
    private bool assignValueStatement;
    private CacheFactory cacheFactory;
    private IParameter tempParam;

    internal SelectStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    internal CacheFactory CacheFactory
    {
      get
      {
        return this.cacheFactory;
      }
    }

    internal int AggregateFunctionCount
    {
      get
      {
        if (this.aggregateExpressions == null)
          return 0;
        return this.aggregateExpressions.Count;
      }
    }

    public override IParameter DoGetParam(string paramName)
    {
      if (this.parent == null)
        return this.tempParam;
      return base.DoGetParam(paramName);
    }

    public override void DoSetReturnParameter(IParameter param)
    {
      if (this.parent == null)
        this.tempParam = param;
      else
        base.DoSetReturnParameter(param);
    }

    public override IParameter DoGetReturnParameter()
    {
      if (this.parent == null)
        return this.tempParam;
      return base.DoGetReturnParameter();
    }

    protected override void DoBeforeParse()
    {
      base.DoBeforeParse();
      this.orderColumns = new SelectStatement.OrderColumnList(this);
      this.groupColumns = new SelectStatement.GroupColumnList(this);
      this.resultColumns = new SelectStatement.ResultColumnList(this);
      this.havingClause = new SelectStatement.HavingClause(this);
    }

    public override long AffectedRows
    {
      get
      {
        return -1;
      }
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      parser.SkipToken(true);
      if (parser.IsToken("ALL"))
        parser.SkipToken(true);
      else if (parser.IsToken("DISTINCT") || parser.IsToken("DISTINCTROW"))
      {
        this.distinct = true;
        parser.SkipToken(true);
      }
      if (parser.IsToken("TOP"))
      {
        parser.SkipToken(true);
        bool flag = false;
        if (parser.IsToken("("))
        {
          flag = true;
          parser.SkipToken(true);
        }
        if (parser.TokenValue.TokenType == TokenType.Unknown && parser.TokenValue.Token != "-")
        {
          this.topCountSig = parser.NextSignature(false, true, -1);
          this.topCount = 0L;
          flag = false;
        }
        else if (parser.TokenValue.TokenType != TokenType.Integer)
          throw new VistaDBSQLException(507, "numeric integer value", parser.TokenValue.RowNo, parser.TokenValue.ColNo);
        if (this.topCountSig == (Signature) null)
          this.topCount = long.Parse(parser.TokenValue.Token);
        if (flag)
        {
          parser.SkipToken(true);
          parser.ExpectedExpression(")");
        }
        if (this.topCount < 0L)
          this.topCount = 0L;
        parser.SkipToken(true);
      }
      this.ParseSelectList(parser);
      if (this.ParseFromClause(parser))
      {
        this.ParseWhereClause(parser);
        this.ParseGroupByClause(parser);
        this.PrepareAggregateFunctions();
        this.ParseHavingClause(parser);
        this.ParseUnionOperator(parser);
        this.ParseOrderByClause(parser);
      }
      else
      {
        this.ParseWhereClause(parser);
        this.PrepareAggregateFunctions();
        this.ParseUnionOperator(parser);
      }
    }

    internal bool AddRow(DataRowType dataRowType, object list, bool throughUnion)
    {
      if (this.hasAggregate && !throughUnion)
        return this.AddAggregateRow();
      if (this.addRowMethod != null)
      {
        if (!this.addRowMethod(dataRowType, list, true))
          return false;
      }
      else
      {
        int visibleColumnCount = this.resultColumns.VisibleColumnCount;
        switch (dataRowType)
        {
          case DataRowType.ResultColumnList:
            SelectStatement.ResultColumnList resultColumnList = (SelectStatement.ResultColumnList) list;
            this.resultTable.Insert();
            for (int index = 0; index < visibleColumnCount; ++index)
              this.resultTable.PutColumn(resultColumnList[index].Signature.Result, index);
            this.resultTable.Post();
            break;
          case DataRowType.ArrayList:
            ArrayList arrayList = (ArrayList) list;
            this.resultTable.Insert();
            for (int index = 0; index < visibleColumnCount; ++index)
              this.resultTable.PutColumn((IColumn) arrayList[index], index);
            this.resultTable.Post();
            break;
          case DataRowType.TableRow:
            Row row = (Row) list;
            this.resultTable.Insert();
            this.resultTable.CurrentRow = row;
            this.resultTable.Post();
            break;
        }
      }
      ++this.affectedRows;
      if (throughUnion || this.GetTopCount() > this.affectedRows)
        return true;
      if (this.ordered)
        return this.unionQuery == null;
      return false;
    }

    private bool EndOfStatement(SQLParser parser)
    {
      if (parser.EndOfText || parser.IsToken(";"))
        return true;
      if (this.parent is IFStatement)
        return parser.IsToken("ELSE");
      return false;
    }

    private void ParseSelectList(SQLParser parser)
    {
      bool flag = false;
      do
      {
        Signature signature = (Signature) null;
        string paramName = (string) null;
        if (flag)
          parser.SkipToken(true);
        if (ParameterSignature.IsParameter(parser.TokenValue.Token))
        {
          signature = parser.NextSignature(false, true, -1);
          if (parser.IsToken("="))
          {
            paramName = signature.Text.Substring(1);
            parser.SkipToken(true);
            this.assignValueStatement = true;
            signature = parser.NextSignature(false, true, 6);
          }
        }
        else if (this.assignValueStatement)
          throw new VistaDBSQLException(642, (string) null, this.lineNo, this.symbolNo);
        if (signature == (Signature) null)
          signature = parser.NextSignature(false, true, 6);
        string str;
        if (parser.IsToken(",") || parser.IsToken("FROM") || (parser.IsToken("WHERE") || parser.IsToken("UNION")) || this.EndOfStatement(parser))
        {
          str = this.resultColumns.GenerateColumnAlias(signature);
        }
        else
        {
          if (parser.IsToken("AS"))
            parser.SkipToken(true);
          str = parser.TokenValue.Token;
          if (parser.TokenValue.ValidateName && parser.TokenValue.TokenType != TokenType.String)
            SQLParser.ValidateNameOrAlias(str, parser.TokenValue.RowNo, parser.TokenValue.ColNo);
          parser.SkipToken(false);
        }
        SelectStatement.ResultColumn column = new SelectStatement.ResultColumn(signature, str, false, paramName);
        this.hasAggregate = this.hasAggregate || column.Aggregate;
        this.resultColumns.Add(column);
        flag = true;
      }
      while (parser.IsToken(","));
    }

    private void ParseOrderByClause(SQLParser parser)
    {
      if (this.addRowMethod != null || !parser.IsToken("ORDER"))
        return;
      parser.SkipToken(true);
      parser.ExpectedExpression("BY");
      string objectName = (string) null;
      string tableName = (string) null;
      int ordinal = -1;
      do
      {
        parser.SkipToken(true);
        OrderDirection orderDirection = OrderDirection.Ascending;
        string token = parser.TokenValue.Token;
        TokenType tokenType = parser.TokenValue.TokenType;
        int rowNo = parser.TokenValue.RowNo;
        int colNo = parser.TokenValue.ColNo;
        bool isOrdinal;
        switch (tokenType)
        {
          case TokenType.Unknown:
          case TokenType.Name:
          case TokenType.ComplexName:
            isOrdinal = false;
            tableName = parser.ParseComplexName(out objectName);
            SelectStatement.ResultColumn resultColumn = this.resultColumns.AddOrderColumn(tableName, objectName, parser);
            if (resultColumn != null && resultColumn.Alias != null && (!resultColumn.Alias.Equals("*", StringComparison.OrdinalIgnoreCase) && !objectName.Equals(resultColumn.Alias, StringComparison.OrdinalIgnoreCase)))
            {
              objectName = resultColumn.Alias;
              break;
            }
            break;
          case TokenType.Integer:
            isOrdinal = true;
            ordinal = int.Parse(token, CrossConversion.NumberFormat);
            break;
          default:
            throw new VistaDBSQLException(567, token, rowNo, colNo);
        }
        if (parser.SkipToken(false))
        {
          if (parser.IsToken("ASC"))
            parser.SkipToken(false);
          else if (parser.IsToken("DESC"))
          {
            orderDirection = OrderDirection.Descending;
            parser.SkipToken(false);
          }
        }
        this.orderColumns.Add(new SelectStatement.OrderColumn(this.orderColumns, token, isOrdinal, ordinal, objectName, tableName, rowNo, colNo, orderDirection));
      }
      while (parser.IsToken(","));
      this.ordered = true;
    }

    private void ParseGroupByClause(SQLParser parser)
    {
      if (!parser.IsToken("GROUP"))
      {
        if (!this.hasAggregate)
          return;
        for (int index = 0; index < this.resultColumns.Count; ++index)
        {
          SelectStatement.ResultColumn resultColumn = this.resultColumns[index];
          if (resultColumn.Aggregate)
          {
            if (resultColumn.Signature.ColumnCount > 0)
              throw new VistaDBSQLException(569, resultColumn.Alias, resultColumn.Signature.LineNo, resultColumn.Signature.SymbolNo);
          }
          else if (resultColumn.Signature.SignatureType != SignatureType.Constant)
            throw new VistaDBSQLException(569, resultColumn.Alias, resultColumn.Signature.LineNo, resultColumn.Signature.SymbolNo);
        }
      }
      else
      {
        this.lineNo = parser.TokenValue.RowNo;
        this.symbolNo = parser.TokenValue.ColNo;
        parser.SkipToken(true);
        parser.ExpectedExpression("BY");
        do
        {
          Signature signature = parser.NextSignature(true, true, 6);
          bool distinct;
          if (signature.HasAggregateFunction(out distinct))
            throw new VistaDBSQLException(574, "", signature.LineNo, signature.SymbolNo);
          if (signature.SignatureType == SignatureType.Constant)
            throw new VistaDBSQLException(580, "", signature.LineNo, signature.SymbolNo);
          SelectStatement.ResultColumn column = this.resultColumns[signature];
          if (column == null)
          {
            string columnAlias = this.resultColumns.GenerateColumnAlias(signature);
            column = new SelectStatement.ResultColumn(signature, columnAlias, true, (string) null);
            this.resultColumns.Add(column);
          }
          this.groupColumns.Add(column, signature.LineNo, signature.SymbolNo);
        }
        while (parser.IsToken(","));
        if (this.hasAggregate)
          return;
        this.distinct = true;
      }
    }

    private void ParseHavingClause(SQLParser parser)
    {
      if (!parser.IsToken("HAVING"))
        return;
      if (!this.hasAggregate)
        throw new VistaDBSQLException(575, "", parser.TokenValue.RowNo, parser.TokenValue.ColNo);
      this.havingClause.Signature = parser.NextSignature(true, true, 6);
    }

    private void PrepareAggregateFunctions()
    {
      if (!this.hasAggregate)
        return;
      List<AggregateFunction> list = new List<AggregateFunction>();
      this.distinctAggregate = false;
      this.aggregateExpressions = new SelectStatement.AggregateExpressionList();
      for (int index = 0; index < this.resultColumns.VisibleColumnCount; ++index)
      {
        SelectStatement.ResultColumn resultColumn = this.resultColumns[index];
        if (resultColumn.Aggregate)
        {
          resultColumn.Signature.GetAggregateFunctions(list);
          foreach (AggregateFunction func in list)
            this.aggregateExpressions.Add(new SelectStatement.AggregateExpression(func, this.resultColumns.GenerateColumnAlias((Signature) null)));
          if (resultColumn.HasDistinctAggregate)
            this.distinctAggregate = true;
          list.Clear();
        }
      }
    }

    private void ParseUnionOperator(SQLParser parser)
    {
      if (!parser.IsToken("UNION"))
        return;
      parser.SkipToken(true);
      if (parser.IsToken("ALL"))
      {
        this.unionAll = true;
        parser.SkipToken(true);
      }
      parser.ExpectedExpression("SELECT");
      this.unionQuery = (SelectStatement) new SelectUnionStatement(this.connection, (Statement) this, parser);
    }

    public override void SetChanged()
    {
      this.resultColumns.SetChanged();
      this.whereClause.SetChanged();
      this.havingClause.SetChanged();
      if (this.join == null)
        return;
      this.join.SetUpdated();
    }

    public void ClearChanged()
    {
      this.resultColumns.ClearChanged();
      this.whereClause.ClearChanged();
      this.havingClause.ClearChanged();
      if (this.join == null)
        return;
      this.join.ClearUpdated();
    }

    private bool AreOnlySimpleColumns(SelectStatement.ResultColumnList resultColumns)
    {
      foreach (SelectStatement.ResultColumn resultColumn in (List<SelectStatement.ResultColumn>) resultColumns)
      {
        if (resultColumn.Aggregate)
          return false;
        Signature signature = resultColumn.Signature;
        if (signature == (Signature) null || signature.SignatureType != SignatureType.Column && signature.SignatureType != SignatureType.MultiplyColumn)
          return false;
      }
      return true;
    }

    private void FindReplaceableLookupTables(IRowSet currentRowset, Dictionary<string, SourceTable> candidates, bool leftmost)
    {
      if (currentRowset == null)
        return;
      Join join = currentRowset as Join;
      if (join != null)
      {
        this.FindReplaceableLookupTables(join.LeftRowSet, candidates, leftmost);
        this.FindReplaceableLookupTables(join.RightRowSet, candidates, false);
      }
      else
      {
        if (leftmost)
          return;
        NativeSourceTable nativeSourceTable = currentRowset as NativeSourceTable;
      }
    }

    protected override VistaDBType OnPrepareQuery()
    {
      if (this.join != null)
      {
        IViewList views = this.Database.EnumViews();
        IVistaDBTableNameCollection tableNames = this.Database.GetTableNames();
        this.cacheFactory = !(this.join is Join) ? (CacheFactory) null : new CacheFactory();
        int tableIndex = 0;
        this.join = this.join.PrepareTables(tableNames, views, this.sourceTables, false, ref tableIndex);
      }
      if (this.topCountSig != (Signature) null)
      {
        int num1 = (int) this.topCountSig.Prepare();
      }
      this.sourceTables.Prepare();
      this.PrepareOptimize();
      this.resultColumns.Prepare();
      if (this.assignValueStatement)
      {
        foreach (SelectStatement.ResultColumn resultColumn in (List<SelectStatement.ResultColumn>) this.resultColumns)
        {
          string paramName = resultColumn.ParamName;
          if (paramName != null && this.parent.DoGetParam(paramName) == null)
            throw new VistaDBSQLException(616, "@" + paramName, this.lineNo, this.symbolNo);
        }
      }
      this.whereClause.Prepare();
      this.orderColumns.Prepare();
      if (this.join != null)
        this.join.Prepare();
      this.groupColumns.Prepare();
      this.havingClause.Prepare();
      this.sourceTables.Unprepare();
      for (int index = 0; index < this.resultColumns.Count; ++index)
        this.resultColumns[index].Width = this.resultColumns[index].Signature.GetWidth();
      if (this.unionQuery != null)
      {
        int num2 = (int) this.unionQuery.PrepareQuery();
        if (this.resultColumns.VisibleColumnCount != this.unionQuery.resultColumns.VisibleColumnCount)
          throw new VistaDBSQLException(577, "", this.unionQuery.resultColumns[0].Signature.LineNo, this.unionQuery.resultColumns[0].Signature.SymbolNo);
        for (int index = 0; index < this.resultColumns.VisibleColumnCount; ++index)
        {
          SelectStatement.ResultColumn resultColumn = this.resultColumns[index];
          if (!Utils.CompatibleTypes(this.unionQuery.resultColumns[index].DataType, resultColumn.DataType))
            throw new VistaDBSQLException(578, "", this.unionQuery.resultColumns[0].Signature.LineNo, this.unionQuery.resultColumns[0].Signature.SymbolNo);
          int width = this.unionQuery.resultColumns[index].Width;
          if (width > this.resultColumns[index].Width)
            this.resultColumns[index].Width = width;
        }
      }
      if (this.resultColumns.Count - this.resultColumns.HiddenSortColumnCount == 1)
        return this.resultColumns[0].DataType;
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      if (this.assignValueStatement)
        this.SetChanged();
      SourceTable table;
      IQueryResult result = this.IsLiveQuery() ? this.ExecuteLiveQuery((Signature[]) null, true, out table) : this.ExecuteNonLiveQuery();
      if (!this.assignValueStatement)
        return result;
      this.CalculateParametersValue(result);
      return (IQueryResult) null;
    }

    protected override bool AcceptRow()
    {
      if (!this.hasAggregate)
        this.resultColumns.Execute();
      return this.AddRow(DataRowType.ResultColumnList, (object) this.resultColumns, false);
    }

    public override IQuerySchemaInfo GetSchemaInfo()
    {
      return (IQuerySchemaInfo) this;
    }

    private long GetTopCount()
    {
      if (this.topCountSig != (Signature) null && this.topCount == 0L)
      {
        IValue destinationValue = (IValue) new BigIntColumn();
        this.Database.Conversion.Convert((IValue) this.topCountSig.Execute(), destinationValue);
        this.topCount = (long) destinationValue.Value;
      }
      if (!this.singleRow)
        return this.topCount;
      return this.topCount <= 0L ? 0L : 1L;
    }

    private void CalculateParametersValue(IQueryResult result)
    {
      try
      {
        if (result.EndOfTable)
          return;
        result.FirstRow();
        while (!result.EndOfTable)
        {
          this.resultColumns.Execute();
          result.NextRow();
        }
        int index = 0;
        for (int columnCount = result.GetColumnCount(); index < columnCount; ++index)
        {
          SelectStatement.ResultColumn resultColumn = this.resultColumns[index];
          if (resultColumn.ParamName != null)
          {
            IParameter parameter = this.DoGetParam(resultColumn.ParamName);
            IColumn column = resultColumn.Signature.CreateColumn(parameter.DataType);
            this.Database.Conversion.Convert(this.IsLiveQuery() ? (IValue) resultColumn.Signature.Result : (IValue) result.GetColumn(index), (IValue) column);
            parameter.Value = ((IValue) column).Value;
          }
        }
      }
      finally
      {
        result.Close();
      }
    }

    private void PreExecute()
    {
      this.Optimize();
      this.simpleAggregateAdded = false;
      this.nonAggColumns = (SelectStatement.ResultColumnList) null;
    }

    private void SubstituteLookupTables()
    {
      if (this.constraintOperations == null || this.cacheFactory == null)
        return;
      TableCollection tableCollection = new TableCollection();
      SourceTable sourceTable1 = (SourceTable) null;
      int num1 = 0;
      bool flag = false;
      foreach (SourceTable sourceTable2 in this.sourceTables.ToArray())
      {
        ColumnSignature optimizedIndexColumn = sourceTable2.OptimizedIndexColumn;
        ColumnSignature optimizedKeyColumn = sourceTable2.OptimizedKeyColumn;
        if ((Signature) optimizedIndexColumn != (Signature) null && (Signature) optimizedKeyColumn != (Signature) null)
        {
          int num2 = (int) optimizedIndexColumn.Prepare();
          int num3 = (int) optimizedKeyColumn.Prepare();
          if (this.cacheFactory.GetLookupTable(VistaDBContext.DDAChannel.CurrentDatabase, sourceTable2, optimizedIndexColumn.ColumnName, optimizedKeyColumn) != null)
          {
            tableCollection.AddTable(sourceTable2);
            this.sourceTables.Remove(sourceTable2);
            flag = true;
            continue;
          }
        }
        if (flag)
        {
          sourceTable1?.SetNextTable(sourceTable2);
          sourceTable2.CollectionOrder = num1;
        }
        ++num1;
        sourceTable1 = sourceTable2;
      }
      if (!flag)
        return;
      sourceTable1?.SetNextTable((SourceTable) null);
      foreach (SelectStatement.ResultColumn resultColumn in (List<SelectStatement.ResultColumn>) this.resultColumns)
      {
        ColumnSignature signature = resultColumn.Signature as ColumnSignature;
        if (!((Signature) signature == (Signature) null))
        {
          KeyedLookupTable lookupTable = this.cacheFactory.GetLookupTable(signature.TableAlias);
          if (lookupTable != null)
          {
            QuickJoinLookupColumn lookupColumn = lookupTable.GetLookupColumn(signature);
            resultColumn.ReplaceSignature(lookupColumn);
          }
        }
      }
      tableCollection.Unprepare();
    }

    internal void SwitchToTemporaryTable(SourceRow sourceRow, int columnIndex, SelectStatement.ResultColumn resultColumn)
    {
      if (this.whereClause == null || this.whereClause.Signature == (Signature) null)
        return;
      this.whereClause.Signature.SwitchToTempTable(sourceRow, columnIndex, resultColumn);
    }

    internal IQueryResult ExecuteLiveQuery(Signature[] signatures, bool readOnly, out SourceTable table)
    {
      table = (SourceTable) null;
      if (this.whereClause.IsAlwaysFalse)
      {
        this.endOfTable = true;
        return (IQueryResult) this;
      }
      this.PreExecute();
      if (signatures != null)
      {
        int index = 0;
        for (int length = signatures.Length; index < length; ++index)
        {
          SelectStatement.ResultColumn resultColumn = this.resultColumns[index];
          signatures[index] = resultColumn.Signature;
          if (!resultColumn.IsExpression)
          {
            if (index == 0)
              table = ((ColumnSignature) resultColumn.Signature).Table;
            else if (table != null && table != ((ColumnSignature) resultColumn.Signature).Table)
              table = (SourceTable) null;
          }
          else
            table = (SourceTable) null;
        }
      }
      if (!readOnly && table != null)
        table.ReadOnly = false;
      this.sourceTables.Open();
      this.FirstRow();
      return (IQueryResult) this;
    }

    internal IQueryResult ExecuteNonLiveQuery()
    {
      this.PreExecute();
      if (this.hasAggregate)
        this.CreateGroupTable();
      else
        this.CreateTempTable();
      this.affectedRows = 0L;
      try
      {
        bool flag = this.whereClause.IsAlwaysFalse;
        if (!flag)
        {
          if (this.sourceTables.Count > 0)
          {
            try
            {
              this.ExecuteJoin();
            }
            finally
            {
              this.Close();
            }
          }
          else
            this.AcceptRow();
          if (this.hasAggregate)
            this.GroupBy();
          else if ((this.unionQuery == null || !this.unionAll) && (this.distinct && this.addRowMethod == null))
            this.resultTable.Sort((QueryResultKey[]) null, true, false);
        }
        if (this.unionQuery != null)
        {
          this.unionQuery.ExecuteQuery();
          if (!this.unionAll && this.addRowMethod == null)
            this.resultTable.Sort((QueryResultKey[]) null, true, false);
          flag = this.resultTable != null && this.resultTable.RowCount > 0L;
        }
        if (flag)
          return (IQueryResult) this.resultTable;
        this.SetChanged();
        if (this.addRowMethod != null)
          return (IQueryResult) null;
        this.OrderBy();
        this.affectedRows = this.resultTable.RowCount;
        return (IQueryResult) this.resultTable;
      }
      finally
      {
        this.groupTable = (TempTable) null;
        this.hashGroupTable = (HashGroupTable) null;
        if (this.resultTable != null)
          this.resultTable.FirstRow();
        this.resultTable = (TempTable) null;
      }
    }

    private bool AddAggregateRow()
    {
      if (this.hashGroupTable != null)
      {
        this.hashGroupTable.AddRowToAggregateStream();
        return true;
      }
      if (this.resultTable == null)
      {
        if (this.simpleAggregateAdded)
        {
          for (int index = 0; index < this.aggregateExpressions.Count; ++index)
          {
            object newVal = (object) null;
            AggregateFunction function = this.aggregateExpressions[index].Function;
            if (function.Expression != (Signature) null)
              newVal = ((IValue) this.aggregateExpressions[index].Expression.Execute()).Value;
            if (!function.AddRowToGroup(newVal))
              return false;
          }
        }
        else
        {
          this.simpleAggregateAdded = true;
          for (int index = 0; index < this.aggregateExpressions.Count; ++index)
          {
            object newVal = (object) null;
            AggregateFunction function = this.aggregateExpressions[index].Function;
            if (function.Expression != (Signature) null)
              newVal = ((IValue) this.aggregateExpressions[index].Expression.Execute()).Value;
            function.CreateNewGroup(newVal);
          }
        }
        return true;
      }
      this.resultTable.Insert();
      for (int index = 0; index < this.nonAggColumns.Count; ++index)
        this.resultTable.PutColumn(this.nonAggColumns[index].Signature.Execute(), index);
      int count = this.nonAggColumns.Count;
      for (int index = 0; index < this.aggregateExpressions.Count; ++index)
      {
        Signature expression = this.aggregateExpressions[index].Expression;
        if (expression != (Signature) null)
        {
          this.resultTable.PutColumn(expression.Execute(), count);
          ++count;
        }
      }
      this.resultTable.Post();
      return true;
    }

    private void CreateTempTable()
    {
      this.resultTable = new TempTable(this.connection.Database);
      for (int index = 0; index < this.resultColumns.VisibleColumnCount; ++index)
        this.resultTable.AddColumn(this.resultColumns[index].Alias, this.resultColumns[index].DataType);
      this.resultTable.FinalizeCreate();
    }

    private void CreateGroupTable()
    {
      if (this.groupColumns.Count == 0 && !this.distinctAggregate)
        return;
      if (this.CanBeUsedHashGroupTable())
      {
        this.hashGroupTable = new HashGroupTable(this.Database, this.addRowMethod, this.resultColumns, this.groupColumns, this.aggregateExpressions, this.havingClause);
        this.resultTable = (TempTable) this.hashGroupTable;
      }
      else
      {
        this.resultTable = new TempTable(this.connection.Database);
        this.nonAggColumns = new SelectStatement.ResultColumnList(this);
        for (int index = 0; index < this.resultColumns.Count; ++index)
        {
          SelectStatement.ResultColumn resultColumn = this.resultColumns[index];
          if (!resultColumn.Aggregate)
          {
            this.resultTable.AddColumn(resultColumn.Alias, resultColumn.Signature.DataType);
            this.nonAggColumns.Add(resultColumn);
          }
        }
        for (int index = 0; index < this.aggregateExpressions.Count; ++index)
        {
          Signature expression = this.aggregateExpressions[index].Expression;
          if (expression != (Signature) null)
            this.resultTable.AddColumn(this.aggregateExpressions[index].Alias, expression.DataType);
        }
        this.resultTable.FinalizeCreate();
        this.groupTable = this.resultTable;
        this.groupTable.Insert();
      }
    }

    private bool CanBeUsedHashGroupTable()
    {
      if (this.distinctAggregate || !this.connection.GetGroupOptimization())
        return false;
      int index = 0;
      for (int count = this.groupColumns.Count; index < count; ++index)
      {
        if (Utils.IsLongDataType(this.groupColumns[index].DataType))
          return false;
      }
      return true;
    }

    private TempTable CreateSecondGroupTable()
    {
      TempTable tempTable = new TempTable(this.connection.Database);
      for (int index = 0; index < this.aggregateExpressions.Count; ++index)
      {
        SelectStatement.AggregateExpression aggregateExpression = this.aggregateExpressions[index];
        tempTable.AddColumn(aggregateExpression.Alias, aggregateExpression.Function.DataType);
      }
      for (int visibleColumnCount = this.resultColumns.VisibleColumnCount; visibleColumnCount < this.resultColumns.Count; ++visibleColumnCount)
      {
        SelectStatement.ResultColumn resultColumn = this.resultColumns[visibleColumnCount];
        tempTable.AddColumn(resultColumn.Alias, resultColumn.Signature.DataType);
      }
      return tempTable;
    }

    private void OrderBy()
    {
      if (this.orderColumns.Count <= 0)
        return;
      QueryResultKey[] sortOrder = new QueryResultKey[this.orderColumns.Count];
      int index = 0;
      for (int count = this.orderColumns.Count; index < count; ++index)
      {
        sortOrder[index].ColumnIndex = this.orderColumns[index].ColumnIndex;
        sortOrder[index].Descending = this.orderColumns[index].OrderDirection == OrderDirection.Descending;
      }
      this.resultTable.Sort(sortOrder, false, false);
      if (this.unionQuery == null)
        this.resultTable.Truncate(this.GetTopCount());
      this.resultTable.FirstRow();
    }

    private void GroupBy()
    {
      if (this.hashGroupTable != null)
      {
        this.hashGroupTable.FinishAggregateStream();
      }
      else
      {
        SourceRow sourceRow1 = new SourceRow();
        SourceRow sourceRow2 = new SourceRow();
        ArrayList list = (ArrayList) null;
        bool flag1 = false;
        List<QueryResultKey[]> queryResultKeyArrayList = (List<QueryResultKey[]>) null;
        List<int> intList = (List<int>) null;
        bool flag2 = false;
        if (this.resultTable == null)
        {
          if (this.simpleAggregateAdded)
          {
            foreach (SelectStatement.AggregateExpression aggregateExpression in (List<SelectStatement.AggregateExpression>) this.aggregateExpressions)
              aggregateExpression.Function.FinishGroup();
          }
          else
          {
            foreach (SelectStatement.AggregateExpression aggregateExpression in (List<SelectStatement.AggregateExpression>) this.aggregateExpressions)
              aggregateExpression.Function.CreateEmptyResult();
          }
          int index1 = 0;
          for (int visibleColumnCount = this.resultColumns.VisibleColumnCount; index1 < visibleColumnCount; ++index1)
            this.resultColumns[index1].Signature.Execute();
          if (this.addRowMethod != null)
          {
            int num = this.addRowMethod(DataRowType.ResultColumnList, (object) this.resultColumns, true) ? 1 : 0;
          }
          else
          {
            this.CreateTempTable();
            this.resultTable.Insert();
            for (int index2 = 0; index2 < this.resultColumns.VisibleColumnCount; ++index2)
              this.resultTable.PutColumn(this.resultColumns[index2].Signature.Result, index2);
            this.resultTable.Post();
          }
        }
        else if (this.resultTable.RowCount == 0L)
        {
          this.groupTable.Close();
          if (this.addRowMethod != null)
            return;
          this.CreateTempTable();
        }
        else
        {
          for (int columnIndex = 0; columnIndex < this.nonAggColumns.Count; ++columnIndex)
            this.nonAggColumns[columnIndex].Signature.SwitchToTempTable(sourceRow1, columnIndex);
          int sortColumnCount = this.resultTable.CurrentRow.Count - this.aggregateExpressions.GetExprCount();
          QueryResultKey[] sortOrder;
          if (this.groupColumns.Count == 0)
          {
            sortOrder = (QueryResultKey[]) null;
          }
          else
          {
            sortOrder = new QueryResultKey[this.groupColumns.Count];
            int index = 0;
            for (int count = this.groupColumns.Count; index < count; ++index)
            {
              sortOrder[index].ColumnIndex = this.nonAggColumns.IndexOf(this.groupColumns[index]);
              sortOrder[index].Descending = false;
            }
          }
          bool manyGroups = sortOrder != null;
          for (int index = 0; index < this.aggregateExpressions.Count; ++index)
          {
            if (this.aggregateExpressions[index].Function.Distinct)
            {
              if (queryResultKeyArrayList == null)
              {
                queryResultKeyArrayList = new List<QueryResultKey[]>();
                intList = new List<int>();
              }
              QueryResultKey[] queryResultKeyArray;
              if (manyGroups)
              {
                queryResultKeyArray = new QueryResultKey[sortOrder.Length + 1];
                sortOrder.CopyTo((Array) queryResultKeyArray, 0);
                queryResultKeyArray[sortOrder.Length].ColumnIndex = index + sortColumnCount;
                queryResultKeyArray[sortOrder.Length].Descending = false;
              }
              else
              {
                queryResultKeyArray = new QueryResultKey[1];
                queryResultKeyArray[0].ColumnIndex = index + sortColumnCount;
                queryResultKeyArray[0].Descending = false;
              }
              queryResultKeyArrayList.Add(queryResultKeyArray);
              intList.Add(index);
            }
            else
              flag1 = true;
          }
          if (this.addRowMethod != null && !this.distinctAggregate)
          {
            list = new ArrayList(this.resultColumns.VisibleColumnCount);
            for (int index = 0; index < this.resultColumns.VisibleColumnCount; ++index)
              list.Add((object) null);
          }
          this.CreateTempTable();
          TempTable secondTable = !this.distinctAggregate ? (TempTable) null : this.CreateSecondGroupTable();
          if (flag1)
          {
            if (manyGroups)
              this.groupTable.Sort(sortOrder, false, false);
            this.AggregateStream(secondTable, sourceRow1, -1, true, list, sortColumnCount, manyGroups, queryResultKeyArrayList == null);
            flag2 = true;
          }
          if (this.distinctAggregate)
          {
            int index1 = 0;
            for (int count = queryResultKeyArrayList.Count; index1 < count; ++index1)
            {
              this.groupTable.Sort(queryResultKeyArrayList[index1], false, true);
              this.AggregateStream(secondTable, sourceRow1, intList[index1], !flag2, list, sortColumnCount, manyGroups, index1 == queryResultKeyArrayList.Count - 1);
              flag2 = true;
            }
            for (int columnIndex = 0; columnIndex < this.resultColumns.VisibleColumnCount; ++columnIndex)
            {
              if (!this.resultColumns[columnIndex].Aggregate)
                this.resultColumns[columnIndex].Signature.SwitchToTempTable(sourceRow1, columnIndex);
            }
            int count1 = this.aggregateExpressions.Count;
            for (int visibleColumnCount = this.resultColumns.VisibleColumnCount; visibleColumnCount < this.resultColumns.Count; ++visibleColumnCount)
            {
              this.resultColumns[visibleColumnCount].Signature.SwitchToTempTable(sourceRow2, count1);
              ++count1;
            }
            for (int columnIndex = 0; columnIndex < this.aggregateExpressions.Count; ++columnIndex)
              this.aggregateExpressions[columnIndex].Function.SwitchToTempTable(sourceRow2, columnIndex);
            this.resultTable.FirstRow();
            secondTable.FirstRow();
            while (!this.resultTable.EndOfTable)
            {
              sourceRow1.Row = (IRow) this.resultTable.CurrentRow;
              sourceRow2.Row = (IRow) secondTable.CurrentRow;
              if (this.havingClause.Evaluate())
              {
                for (int index2 = 0; index2 < this.resultColumns.VisibleColumnCount; ++index2)
                {
                  SelectStatement.ResultColumn resultColumn = this.resultColumns[index2];
                  if (resultColumn.Aggregate)
                    ((IValue) sourceRow1.Row[index2]).Value = ((IValue) resultColumn.Signature.Execute()).Value;
                }
                if (this.addRowMethod != null)
                {
                  int num = this.addRowMethod(DataRowType.TableRow, (object) sourceRow1.Row, true) ? 1 : 0;
                }
                this.resultTable.NextRow();
              }
              else
              {
                if (this.addRowMethod == null)
                  this.resultTable.Delete();
                else
                  this.resultTable.NextRow();
                --this.affectedRows;
              }
              secondTable.NextRow();
            }
            secondTable.Close();
            if (this.addRowMethod != null)
              this.resultTable.Close();
          }
          for (int index = 0; index < this.nonAggColumns.Count; ++index)
            this.nonAggColumns[index].Signature.SwitchToTable();
          for (int index = 0; index < this.aggregateExpressions.Count; ++index)
            this.aggregateExpressions[index].Function.SwitchToTable();
          this.groupTable.Close();
        }
      }
    }

    private void AggregateStream(TempTable secondTable, SourceRow sourceRow, int functionIndex, bool insertNew, ArrayList list, int sortColumnCount, bool manyGroups, bool lastGrouping)
    {
      bool flag1 = false;
      IVistaDBRow vistaDbRow1 = (IVistaDBRow) null;
      IVistaDBRow vistaDbRow2 = (IVistaDBRow) null;
      bool insertIntoTemp = this.addRowMethod == null || list == null;
      bool flag2 = functionIndex >= 0;
      this.affectedRows = 0L;
      this.groupTable.FirstRow();
      if (!insertNew)
      {
        if (this.distinctAggregate)
          secondTable.FirstRow();
        else
          this.resultTable.FirstRow();
      }
      while (true)
      {
        bool flag3 = this.groupTable.EndOfTable;
        if (!flag3)
        {
          if (flag1)
          {
            if (manyGroups)
            {
              vistaDbRow2 = (IVistaDBRow) this.groupTable.GetCurrentKeyClone();
              if (!flag2)
              {
                if (!vistaDbRow2.Equals((object) vistaDbRow1))
                  flag3 = true;
              }
              else
              {
                for (int index = 0; index < vistaDbRow2.Count - 1; ++index)
                {
                  if (vistaDbRow2[index].Compare(vistaDbRow1[index]) != 0)
                  {
                    flag3 = true;
                    break;
                  }
                }
              }
            }
          }
          else
          {
            vistaDbRow1 = manyGroups ? (IVistaDBRow) this.groupTable.GetCurrentKeyClone() : (IVistaDBRow) null;
            sourceRow.Row = this.groupTable.GetCurrentRowClone();
          }
        }
        if (flag3 || !flag1)
        {
          if (flag1)
          {
            if (this.AddRowFromAggregateStream(secondTable, functionIndex, insertIntoTemp, insertNew, list, sourceRow))
            {
              if (!this.groupTable.EndOfTable)
              {
                vistaDbRow1 = vistaDbRow2;
                sourceRow.Row = this.groupTable.GetCurrentRowClone();
              }
            }
            else
              break;
          }
          else
            flag1 = true;
          this.CreateNewAggregateGroup(functionIndex, sortColumnCount);
        }
        else
          this.AddRowToAggregateGroup(functionIndex, sortColumnCount);
        if (!this.groupTable.EndOfTable)
          this.groupTable.NextRow();
        else
          goto label_20;
      }
      return;
label_20:;
    }

    private bool AddRowFromAggregateStream(TempTable secondTable, int functionIndex, bool insertIntoTemp, bool insertNew, ArrayList list, SourceRow sourceRow)
    {
      if (functionIndex >= 0)
      {
        this.aggregateExpressions[functionIndex].Function.FinishGroup();
      }
      else
      {
        foreach (SelectStatement.AggregateExpression aggregateExpression in (List<SelectStatement.AggregateExpression>) this.aggregateExpressions)
        {
          if (!aggregateExpression.Function.Distinct)
            aggregateExpression.Function.FinishGroup();
        }
      }
      if (!this.distinctAggregate && !this.havingClause.Evaluate())
        return true;
      if (insertIntoTemp && insertNew)
      {
        this.resultTable.Insert();
        if (this.distinctAggregate)
          secondTable.Insert();
      }
      int index1 = 0;
      if (insertNew || !this.distinctAggregate)
      {
        for (int index2 = 0; index2 < this.resultColumns.VisibleColumnCount; ++index2)
        {
          SelectStatement.ResultColumn resultColumn = this.resultColumns[index2];
          if (!resultColumn.Aggregate)
          {
            if (insertNew)
            {
              if (insertIntoTemp)
                this.resultTable.PutColumn(sourceRow.Row[index1], index2);
              else
                list[index2] = (object) sourceRow.Row[index1];
              ++index1;
            }
          }
          else if (!this.distinctAggregate)
          {
            if (insertIntoTemp)
              this.resultTable.PutColumn(resultColumn.Signature.Execute(), index2);
            else
              list[index2] = (object) resultColumn.Signature.Execute();
          }
        }
      }
      if (this.distinctAggregate)
      {
        for (int index2 = 0; index2 < this.aggregateExpressions.Count; ++index2)
        {
          AggregateFunction function = this.aggregateExpressions[index2].Function;
          if (functionIndex < 0 && !function.Distinct || functionIndex == index2)
            secondTable.PutColumn(function.Result, index2);
        }
        int count = this.aggregateExpressions.Count;
        for (int visibleColumnCount = this.resultColumns.VisibleColumnCount; visibleColumnCount < this.resultColumns.Count; ++visibleColumnCount)
        {
          SelectStatement.ResultColumn resultColumn = this.resultColumns[visibleColumnCount];
          secondTable.PutColumn(resultColumn.Signature.Execute(), count);
          ++count;
        }
        secondTable.Post();
        if (!insertNew)
          secondTable.NextRow();
        else
          this.resultTable.Post();
      }
      else if (insertIntoTemp)
      {
        this.resultTable.Post();
        if (!insertNew)
          this.resultTable.NextRow();
      }
      else
      {
        int num = this.addRowMethod(DataRowType.ArrayList, (object) list, true) ? 1 : 0;
      }
      ++this.affectedRows;
      if (this.GetTopCount() > this.affectedRows)
        return true;
      if (this.ordered)
        return this.unionQuery == null;
      return false;
    }

    private void CreateNewAggregateGroup(int functionIndex, int sortColumnCount)
    {
      int index1 = sortColumnCount;
      for (int index2 = 0; index2 < this.aggregateExpressions.Count; ++index2)
      {
        AggregateFunction function = this.aggregateExpressions[index2].Function;
        if (function.Expression == (Signature) null)
        {
          if (functionIndex < 0 && !function.Distinct || functionIndex == index2)
            function.CreateNewGroup((object) null);
        }
        else
        {
          if (functionIndex < 0 && !function.Distinct || functionIndex == index2)
          {
            object newVal = ((IValue) this.groupTable.GetColumn(index1)).Value;
            function.CreateNewGroup(newVal);
          }
          ++index1;
        }
      }
    }

    private void AddRowToAggregateGroup(int functionIndex, int sortColumnCount)
    {
      int index1 = sortColumnCount;
      for (int index2 = 0; index2 < this.aggregateExpressions.Count; ++index2)
      {
        AggregateFunction function = this.aggregateExpressions[index2].Function;
        if (function.Expression == (Signature) null)
        {
          if (functionIndex < 0 && !function.Distinct || functionIndex == index2)
            function.AddRowToGroup((object) null);
        }
        else
        {
          if (functionIndex < 0 && !function.Distinct || functionIndex == index2)
          {
            object newVal = ((IValue) this.groupTable.GetColumn(index1)).Value;
            function.AddRowToGroup(newVal);
          }
          ++index1;
        }
      }
    }

    public bool IsEquals(SelectStatement statement)
    {
      if (this.distinct == statement.distinct && this.resultColumns.IsEquals(statement.resultColumns) && (this.join.IsEquals(statement.join) && this.whereClause.IsEquals(statement.whereClause)) && (this.groupColumns.IsEquals(statement.groupColumns) && this.havingClause.IsEquals(statement.havingClause)))
        return this.orderColumns.IsEquals(statement.orderColumns);
      return false;
    }

    public bool GetIsChanged()
    {
      if (!this.resultColumns.GetIsChanged() && (this.join == null || !this.join.RowUpdated) && !this.whereClause.GetIsChanged())
        return this.havingClause.GetIsChanged();
      return true;
    }

    public bool IsLiveQuery()
    {
      if (this.join != null && !this.hasAggregate && (this.orderColumns.Count == 0 && this.unionQuery == null) && this.addRowMethod == null)
        return !this.distinct;
      return false;
    }

    public void FreeTables()
    {
      this.sourceTables.Free();
      this.resultColumns.SetChanged();
      this.whereClause.SetChanged();
      this.havingClause.SetChanged();
      if (this.join == null)
        return;
      this.join.SetUpdated();
    }

    public int ResultColumnCount
    {
      get
      {
        return this.resultColumns.Count;
      }
    }

    public bool UnionAll
    {
      get
      {
        return this.unionAll;
      }
    }

    public SelectStatement UnionQuery
    {
      get
      {
        return this.unionQuery;
      }
    }

    public bool HasAggregate
    {
      get
      {
        return this.hasAggregate;
      }
    }

    public bool Distinct
    {
      get
      {
        return this.distinct;
      }
    }

    internal bool IsSetTopCount
    {
      get
      {
        if (this.topCount == long.MaxValue)
          return this.topCountSig != (Signature) null;
        return true;
      }
    }

    public bool Sorted
    {
      get
      {
        return this.orderColumns.Count > 0;
      }
    }

    public bool HasWhereClause
    {
      get
      {
        return this.whereClause.Signature != (Signature) null;
      }
    }

    public bool HasOrderByClause
    {
      get
      {
        if (this.orderColumns != null)
          return this.orderColumns.Count > 0;
        return false;
      }
    }

    public bool HasHavingClause
    {
      get
      {
        return this.havingClause.Signature != (Signature) null;
      }
    }

    public bool HasGroupByClause
    {
      get
      {
        if (this.groupColumns != null)
          return this.groupColumns.Count > 0;
        return false;
      }
    }

    public string GetAliasName(int ordinal)
    {
      return this.resultColumns[ordinal].Alias;
    }

    public int GetColumnOrdinal(string name)
    {
      return this.resultColumns.IndexOf(name);
    }

    public int GetWidth(int ordinal)
    {
      return this.resultColumns[ordinal].Width;
    }

    public bool GetIsKey(int ordinal)
    {
      return this.resultColumns[ordinal].IsKey;
    }

    public string GetColumnName(int ordinal)
    {
      return this.resultColumns[ordinal].ColumnName;
    }

    public string GetTableName(int ordinal)
    {
      return this.resultColumns[ordinal].TableName;
    }

    public Type GetColumnType(int ordinal)
    {
      return this.resultColumns[ordinal].SystemType;
    }

    public bool GetIsAllowNull(int ordinal)
    {
      return this.resultColumns[ordinal].IsAllowNull;
    }

    public VistaDBType GetColumnVistaDBType(int ordinal)
    {
      return this.resultColumns[ordinal].DataType;
    }

    public bool GetIsAliased(int ordinal)
    {
      return this.resultColumns[ordinal].IsAliased;
    }

    public bool GetIsExpression(int ordinal)
    {
      return this.resultColumns[ordinal].IsExpression;
    }

    public bool GetIsAutoIncrement(int ordinal)
    {
      return this.resultColumns[ordinal].IsAutoIncrement;
    }

    public bool GetIsLong(int ordinal)
    {
      return this.resultColumns[ordinal].IsLong;
    }

    public bool GetIsReadOnly(int ordinal)
    {
      return this.resultColumns[ordinal].IsReadOnly;
    }

    public string GetDataTypeName(int ordinal)
    {
      return this.resultColumns[ordinal].DataType.ToString();
    }

    public DataTable GetSchemaTable()
    {
      if (!this.prepared)
        return (DataTable) null;
      DataTable schemaTableInstance = BaseSelectStatement.GetSchemaTableInstance();
      schemaTableInstance.BeginLoadData();
      int index = 0;
      for (int columnCount = this.ColumnCount; index < columnCount; ++index)
      {
        SelectStatement.ResultColumn resultColumn = this.resultColumns[index];
        DataRow row = schemaTableInstance.NewRow();
        row["ColumnName"] = (object) resultColumn.Alias;
        row["ColumnOrdinal"] = (object) index;
        row["ColumnSize"] = (object) resultColumn.Width;
        row["NumericPrecision"] = (object) (int) byte.MaxValue;
        row["NumericScale"] = (object) (int) byte.MaxValue;
        row["IsUnique"] = (object) resultColumn.IsKey;
        row["IsKey"] = (object) resultColumn.IsKey;
        row["BaseColumnName"] = (object) resultColumn.ColumnName;
        row["BaseSchemaName"] = (object) null;
        row["BaseTableName"] = (object) resultColumn.TableName;
        row["DataType"] = (object) resultColumn.SystemType;
        row["AllowDBNull"] = (object) resultColumn.IsAllowNull;
        row["ProviderType"] = (object) resultColumn.DataType;
        row["IsAliased"] = (object) resultColumn.IsAliased;
        row["IsExpression"] = (object) resultColumn.IsExpression;
        row["IsIdentity"] = (object) resultColumn.IsAutoIncrement;
        row["IsAutoIncrement"] = (object) resultColumn.IsAutoIncrement;
        row["IsRowVersion"] = (object) false;
        row["IsHidden"] = (object) false;
        row["IsLong"] = (object) resultColumn.IsLong;
        row["IsReadOnly"] = (object) resultColumn.IsReadOnly;
        row["ProviderSpecificDataType"] = (object) resultColumn.SystemType;
        row["DataTypeName"] = (object) resultColumn.DataType.ToString();
        schemaTableInstance.Rows.Add(row);
      }
      schemaTableInstance.AcceptChanges();
      schemaTableInstance.EndLoadData();
      return schemaTableInstance;
    }

    public string GetColumnDescription(int ordinal)
    {
      return this.resultColumns[ordinal].Description;
    }

    public string GetColumnCaption(int ordinal)
    {
      return this.resultColumns[ordinal].Caption;
    }

    public bool GetIsEncrypted(int ordinal)
    {
      return this.resultColumns[ordinal].Encrypted;
    }

    public int GetCodePage(int ordinal)
    {
      return this.resultColumns[ordinal].CodePage;
    }

    public string GetIdentity(int ordinal, out string step, out string seed)
    {
      SelectStatement.ResultColumn resultColumn = this.resultColumns[ordinal];
      step = resultColumn.IdentityStep;
      seed = resultColumn.IdentitySeed;
      return resultColumn.Identity;
    }

    public string GetDefaultValue(int ordinal, out bool useInUpdate)
    {
      SelectStatement.ResultColumn resultColumn = this.resultColumns[ordinal];
      useInUpdate = resultColumn.UseInUpdate;
      return resultColumn.DefaultValue;
    }

    public int ColumnCount
    {
      get
      {
        return this.resultColumns.VisibleColumnCount - this.resultColumns.HiddenSortColumnCount;
      }
    }

    public void FirstRow()
    {
      if (this.sourceTables.Count == 0 || this.GetTopCount() == 0L)
      {
        this.endOfTable = true;
        this.affectedRows = 0L;
      }
      else
      {
        if (!this.sourceTables.AllOpen && this.sourceTables.HasNative)
          this.sourceTables.Open();
        this.endOfTable = !this.sourceTables[0].First(this.constraintOperations) || !this.AcceptJoinedRow();
        this.affectedRows = this.endOfTable ? 0L : 1L;
      }
    }

    public void NextRow()
    {
      if (this.sourceTables.Count == 0 || this.GetTopCount() <= this.affectedRows)
      {
        this.endOfTable = true;
      }
      else
      {
        this.endOfTable = !this.join.Next(this.constraintOperations) || !this.AcceptJoinedRow();
        if (this.endOfTable)
          return;
        ++this.affectedRows;
      }
    }

    public void Close()
    {
      if (this.connection != null)
      {
        if (this.cacheFactory != null)
          this.cacheFactory.Close();
        this.sourceTables.Free();
      }
      this.SetChanged();
      this.endOfTable = true;
    }

    public object GetValue(int index, VistaDBType dataType)
    {
      if (this.endOfTable)
        return (object) null;
      IColumn column1 = this.resultColumns[index].Signature.Execute();
      if (dataType == VistaDBType.Unknown || column1.InternalType == dataType)
        return ((IValue) column1).Value;
      IColumn column2 = this.resultColumns[index].Signature.CreateColumn(dataType);
      this.Database.Conversion.Convert((IValue) column1, (IValue) column2);
      return ((IValue) column2).Value;
    }

    public IColumn GetColumn(int index)
    {
      if (!this.endOfTable)
        return this.resultColumns[index].Signature.Execute();
      return (IColumn) null;
    }

    public bool IsNull(int index)
    {
      return this.resultColumns[index].Signature.Execute().IsNull;
    }

    public int GetColumnCount()
    {
      return this.resultColumns.VisibleColumnCount;
    }

    public bool EndOfTable
    {
      get
      {
        return this.endOfTable;
      }
    }

    public long RowCount
    {
      get
      {
        return this.affectedRows;
      }
    }

    public long TickCount
    {
      get
      {
        return 0;
      }
      set
      {
      }
    }

    internal delegate bool AddRowMethod(DataRowType dataRowType, object list, bool throughUnion);

    internal class ResultColumn
    {
      private Signature signature;
      private string alias;
      private bool hidden;
      private bool aggregate;
      private int width;
      private bool aggregateDistinct;
      private string columnName;
      private string tableName;
      private string tableAlias;
      private bool isKey;
      private bool isAllowNull;
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
      private string paramName;

      public ResultColumn(Signature signature, string alias, bool hidden, string paramName)
      {
        this.signature = signature;
        this.alias = alias;
        this.hidden = hidden;
        this.aggregate = signature.HasAggregateFunction(out this.aggregateDistinct);
        this.width = 0;
        this.columnName = (string) null;
        this.tableName = (string) null;
        this.isKey = false;
        this.isAllowNull = true;
        this.isExpression = true;
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
        this.paramName = paramName;
      }

      public ResultColumn(ColumnSignature column, string alias)
        : this((Signature) column, alias, false, (string) null)
      {
        this.columnName = column.ColumnName;
        this.tableName = column.Table.TableName;
        this.tableAlias = column.Table.Alias;
        this.isKey = column.IsKey;
        this.isAllowNull = column.IsAllowNull;
        this.isExpression = column.IsExpression;
        this.isAutoIncrement = column.IsAutoIncrement;
        this.isReadOnly = column.IsReadOnly;
        this.description = column.Description;
        this.caption = column.Caption;
        this.encrypted = column.Encrypted;
        this.codePage = column.CodePage;
        this.identity = column.Identity;
        this.identityStep = column.IdentityStep;
        this.identitySeed = column.IdentitySeed;
        this.defaultValue = column.DefaultValue;
        this.useInUpdate = column.UseInUpdate;
      }

      public bool Prepare()
      {
        SignatureType signatureType;
        try
        {
          signatureType = this.signature.Prepare();
        }
        catch (Exception ex)
        {
          throw new VistaDBSQLException(ex, 662, this.alias, this.signature.LineNo, this.signature.SymbolNo);
        }
        switch (signatureType)
        {
          case SignatureType.Constant:
            if (this.signature.SignatureType != SignatureType.Constant)
            {
              this.signature = (Signature) ConstantSignature.CreateSignature(this.signature.Execute(), this.signature.Parent);
              break;
            }
            goto default;
          case SignatureType.MultiplyColumn:
            return false;
          default:
            if (this.signature.DataType == VistaDBType.Unknown)
              throw new VistaDBSQLException(561, this.alias, this.signature.LineNo, this.signature.SymbolNo);
            break;
        }
        this.isAllowNull = this.signature.IsAllowNull;
        if (this.signature.SignatureType == SignatureType.Column || this.signature.SignatureType == SignatureType.ExternalColumn)
        {
          ColumnSignature signature = (ColumnSignature) this.signature;
          if (!signature.IsExpression)
          {
            this.columnName = signature.ColumnName;
            this.tableName = signature.Table.TableName;
            this.tableAlias = signature.Table.Alias;
            this.isKey = signature.IsKey;
            this.isAllowNull = signature.IsAllowNull;
            this.isExpression = signature.IsExpression;
            this.isAutoIncrement = signature.IsAutoIncrement;
            this.isReadOnly = signature.IsReadOnly;
            this.description = signature.Description;
            this.caption = signature.Caption;
            this.encrypted = signature.Encrypted;
            this.codePage = signature.CodePage;
            this.identity = signature.Identity;
            this.identityStep = signature.IdentityStep;
            this.identitySeed = signature.IdentitySeed;
            this.defaultValue = signature.DefaultValue;
            this.useInUpdate = signature.UseInUpdate;
          }
        }
        return true;
      }

      public Signature Signature
      {
        get
        {
          return this.signature;
        }
      }

      public string Alias
      {
        get
        {
          return this.alias;
        }
      }

      public bool Hidden
      {
        get
        {
          return this.hidden;
        }
        set
        {
          this.hidden = value;
        }
      }

      public bool Aggregate
      {
        get
        {
          return this.aggregate;
        }
      }

      public bool HasDistinctAggregate
      {
        get
        {
          return this.aggregateDistinct;
        }
      }

      public VistaDBType DataType
      {
        get
        {
          return this.signature.DataType;
        }
      }

      public int Width
      {
        get
        {
          return this.width;
        }
        set
        {
          this.width = value;
        }
      }

      public string ColumnName
      {
        get
        {
          return this.columnName;
        }
      }

      public string TableName
      {
        get
        {
          return this.tableName;
        }
      }

      public string TableAlias
      {
        get
        {
          return this.tableAlias;
        }
      }

      public bool IsKey
      {
        get
        {
          return this.isKey;
        }
      }

      public bool IsAllowNull
      {
        get
        {
          return this.isAllowNull;
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

      public string ParamName
      {
        get
        {
          return this.paramName;
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

      public bool IsAliased
      {
        get
        {
          if (!this.isExpression)
            return this.signature.Parent.Connection.CompareString(this.alias, this.columnName, true) != 0;
          return true;
        }
      }

      public Type SystemType
      {
        get
        {
          return Utils.GetSystemType(this.DataType);
        }
      }

      public bool IsLong
      {
        get
        {
          switch (this.DataType)
          {
            case VistaDBType.Text:
            case VistaDBType.NText:
            case VistaDBType.Image:
            case VistaDBType.VarBinary:
              return true;
            default:
              return false;
          }
        }
      }

      public bool IsEquals(SelectStatement.ResultColumn column)
      {
        if (this.DataType == column.DataType)
          return this.signature == column.signature;
        return false;
      }

      public bool GetIsChanged()
      {
        return this.signature.GetIsChanged();
      }

      internal void ReplaceSignature(QuickJoinLookupColumn newColumnSignature)
      {
        if (!((Signature) newColumnSignature != (Signature) null))
          return;
        this.signature = (Signature) newColumnSignature;
      }
    }

    internal class ResultColumnList : List<SelectStatement.ResultColumn>
    {
      private SelectStatement parent;
      private int visibleColumnCount;
      private int hiddenSortColumnCount;

      public ResultColumnList(SelectStatement parent)
      {
        this.parent = parent;
        this.visibleColumnCount = 0;
        this.hiddenSortColumnCount = 0;
      }

      public int Add(SelectStatement.ResultColumn column)
      {
        if (!column.Hidden)
          ++this.visibleColumnCount;
        base.Add(column);
        return this.Count - 1;
      }

      public new void Insert(int index, SelectStatement.ResultColumn column)
      {
        if (!column.Hidden)
          ++this.visibleColumnCount;
        base.Insert(index, column);
      }

      public int IndexOf(string alias)
      {
        for (int index = 0; index < this.Count; ++index)
        {
          if (this.parent.Connection.CompareString(alias, base[index].Alias, true) == 0)
            return index;
        }
        return -1;
      }

      public int IndexOf(Signature signature)
      {
        for (int index = 0; index < this.Count; ++index)
        {
          if (base[index].Signature == signature)
            return index;
        }
        return -1;
      }

      public SelectStatement.ResultColumn AddOrderColumn(string tableName, string columnName, SQLParser parser)
      {
        SelectStatement.ResultColumn column1 = (SelectStatement.ResultColumn) null;
        int index1 = -1;
        for (int index2 = 0; index2 < this.Count; ++index2)
        {
          column1 = base[index2];
          if (tableName == null && this.parent.Connection.CompareString(columnName, column1.Alias, true) == 0)
          {
            index1 = index2;
            break;
          }
          Signature signature = column1.Signature;
          ColumnSignature columnSignature;
          if (signature.SignatureType == SignatureType.Column || signature.SignatureType == SignatureType.ExternalColumn)
          {
            columnSignature = (ColumnSignature) signature;
            if ((tableName == null || this.parent.Connection.CompareString(tableName, column1.TableName, true) == 0) && this.parent.Connection.CompareString(columnName, column1.Alias, true) == 0)
            {
              index1 = index2;
              break;
            }
          }
          else if (signature.SignatureType == SignatureType.MultiplyColumn)
          {
            columnSignature = (ColumnSignature) signature;
            if (tableName == null || column1.TableName == null || this.parent.Connection.CompareString(tableName, column1.TableName, true) == 0)
            {
              index1 = index2;
              break;
            }
          }
        }
        if (index1 >= 0)
        {
          if (index1 > this.visibleColumnCount)
          {
            column1 = this[index1];
            column1.Hidden = false;
            this.RemoveAt(index1);
            this.Insert(this.visibleColumnCount, column1);
          }
          return column1;
        }
        ColumnSignature signature1 = ColumnSignature.CreateSignature(tableName, columnName, parser);
        string columnAlias = this.GenerateColumnAlias((Signature) signature1);
        SelectStatement.ResultColumn column2 = new SelectStatement.ResultColumn((Signature) signature1, columnAlias, false, (string) null);
        this.Insert(this.visibleColumnCount, column2);
        ++this.hiddenSortColumnCount;
        return column2;
      }

      public bool IsEquals(SelectStatement.ResultColumnList list)
      {
        if (this.Count != list.Count)
          return false;
        for (int index = 0; index < this.Count; ++index)
        {
          if (!this[index].IsEquals(list[index]))
            return false;
        }
        return true;
      }

      public void Execute()
      {
        for (int index = 0; index < this.Count; ++index)
          this[index].Signature.Execute();
      }

      public void Prepare()
      {
        List<ColumnSignature> columnSignatureList = new List<ColumnSignature>();
        int index1 = 0;
        for (int count1 = this.Count; index1 < count1; ++index1)
        {
          SelectStatement.ResultColumn resultColumn = this[index1];
          if (!resultColumn.Prepare())
          {
            --this.visibleColumnCount;
            ((ColumnSignature) resultColumn.Signature).ExtractColumns(columnSignatureList);
            this.RemoveAt(index1);
            int count2 = columnSignatureList.Count;
            for (int index2 = 0; index2 < count2; ++index2)
            {
              ColumnSignature column = columnSignatureList[index2];
              string columnAlias = this.GenerateColumnAlias((Signature) column);
              this.Insert(index1 + index2, new SelectStatement.ResultColumn(column, columnAlias));
            }
            int num = count2 - 1;
            count1 += num;
            index1 += num;
            columnSignatureList.Clear();
          }
        }
        if (!this.parent.hasAggregate)
          return;
        for (int index2 = 0; index2 < this.visibleColumnCount; ++index2)
        {
          SelectStatement.ResultColumn resultColumn1 = this[index2];
          if (resultColumn1.Aggregate)
          {
            int columnCount1 = resultColumn1.Signature.ColumnCount;
            int columnCount2 = 0;
            foreach (SelectStatement.ResultColumn resultColumn2 in (List<SelectStatement.ResultColumn>) this)
            {
              if (!resultColumn2.Aggregate)
                resultColumn1.Signature.Relink(resultColumn2.Signature, ref columnCount2);
            }
            if (columnCount2 > columnCount1)
              throw new VistaDBSQLException(562, resultColumn1.Alias, resultColumn1.Signature.LineNo, resultColumn1.Signature.SymbolNo);
          }
        }
      }

      public string GenerateColumnAlias(Signature signature)
      {
        if (signature != (Signature) null && signature.SignatureType == SignatureType.MultiplyColumn)
          return "*";
        int num = 0;
        string str;
        string alias;
        if (signature != (Signature) null && (signature.SignatureType == SignatureType.Column || signature.SignatureType == SignatureType.ExternalColumn))
        {
          str = ((ColumnSignature) signature).ColumnName;
          alias = str;
        }
        else
        {
          ++num;
          str = "Column";
          alias = "Column_" + num.ToString();
        }
        for (; this.IndexOf(alias) >= 0; alias = str + "_" + num.ToString())
          ++num;
        return alias;
      }

      public SelectStatement.ResultColumn GetColumnByComplexName(string tableName, string columnName)
      {
        foreach (SelectStatement.ResultColumn resultColumn in (List<SelectStatement.ResultColumn>) this)
        {
          if (this.parent.connection.CompareString(resultColumn.TableAlias, tableName, true) == 0 && this.parent.connection.CompareString(resultColumn.ColumnName, columnName, true) == 0 || this.parent.connection.CompareString(resultColumn.TableName, tableName, true) == 0 && this.parent.connection.CompareString(resultColumn.ColumnName, columnName, true) == 0 || (this.parent.connection.CompareString(resultColumn.Alias, columnName, true) == 0 && (string.IsNullOrEmpty(resultColumn.TableName) || this.parent.connection.CompareString(resultColumn.TableName, tableName, true) == 0) || this.parent.connection.CompareString(resultColumn.Alias, columnName, true) == 0 && this.parent.connection.CompareString(resultColumn.TableAlias, tableName, true) == 0))
            return resultColumn;
        }
        return (SelectStatement.ResultColumn) null;
      }

      public void SetChanged()
      {
        for (int index = 0; index < this.Count; ++index)
          this[index].Signature.SetChanged();
      }

      public void ClearChanged()
      {
        for (int index = 0; index < this.Count; ++index)
          this[index].Signature.ClearChanged();
      }

      public new SelectStatement.ResultColumn this[int index]
      {
        get
        {
          if (index < 0 || index >= this.Count)
            return (SelectStatement.ResultColumn) null;
          return base[index];
        }
      }

      public SelectStatement.ResultColumn this[string alias]
      {
        get
        {
          int index = this.IndexOf(alias);
          if (index < 0)
            return (SelectStatement.ResultColumn) null;
          return base[index];
        }
      }

      public SelectStatement.ResultColumn this[Signature signature]
      {
        get
        {
          int index = this.IndexOf(signature);
          if (index < 0)
            return (SelectStatement.ResultColumn) null;
          return base[index];
        }
      }

      public bool GetIsChanged()
      {
        for (int index = 0; index < this.Count; ++index)
        {
          if (this[index].GetIsChanged())
            return true;
        }
        return false;
      }

      public int VisibleColumnCount
      {
        get
        {
          return this.visibleColumnCount;
        }
      }

      public int HiddenSortColumnCount
      {
        get
        {
          return this.hiddenSortColumnCount;
        }
      }
    }

    private class OrderColumn
    {
      private SelectStatement.OrderColumnList parent;
      private SelectStatement.ResultColumn column;
      private OrderDirection orderDirection;
      private int lineNo;
      private int symbolNo;
      private bool isOrdinal;
      private int ordinal;
      private string columnName;
      private string tableName;
      private string text;
      private int columnIndex;

      public OrderColumn(SelectStatement.OrderColumnList parent, string text, bool isOrdinal, int ordinal, string columnName, string tableName, int lineNo, int symbolNo, OrderDirection orderDirection)
      {
        this.parent = parent;
        this.lineNo = lineNo;
        this.symbolNo = symbolNo;
        this.orderDirection = orderDirection;
        this.column = (SelectStatement.ResultColumn) null;
        this.text = text;
        this.isOrdinal = isOrdinal;
        this.ordinal = ordinal;
        this.columnName = columnName;
        this.tableName = tableName;
        this.columnIndex = -1;
      }

      public bool IsEquals(SelectStatement.OrderColumn column)
      {
        if (this.column == null || column.column == null)
        {
          if (this.isOrdinal == column.isOrdinal && this.ordinal == column.ordinal && (this.parent.Parent.Connection.CompareString(this.columnName, column.columnName, true) == 0 && this.parent.Parent.Connection.CompareString(this.tableName, column.tableName, true) == 0))
            return this.orderDirection == column.orderDirection;
          return false;
        }
        if (this.column.IsEquals(column.column))
          return this.orderDirection == column.orderDirection;
        return false;
      }

      public void Prepare()
      {
        this.column = !this.isOrdinal ? (this.tableName != null ? this.parent.Parent.resultColumns.GetColumnByComplexName(this.tableName, this.columnName) : this.parent.Parent.resultColumns[this.columnName]) : this.parent.Parent.resultColumns[this.ordinal - 1];
        if (this.column == null)
          throw new VistaDBSQLException(567, this.text, this.lineNo, this.symbolNo);
        this.columnIndex = this.parent.Parent.resultColumns.IndexOf(this.column);
      }

      public SelectStatement.ResultColumn Column
      {
        get
        {
          return this.column;
        }
      }

      public OrderDirection OrderDirection
      {
        get
        {
          return this.orderDirection;
        }
      }

      public int ColumnIndex
      {
        get
        {
          return this.columnIndex;
        }
      }
    }

    private class OrderColumnList : List<SelectStatement.OrderColumn>
    {
      private SelectStatement parent;

      public OrderColumnList(SelectStatement parent)
      {
        this.parent = parent;
      }

      public SelectStatement Parent
      {
        get
        {
          return this.parent;
        }
      }

      public bool IsEquals(SelectStatement.OrderColumnList list)
      {
        if (this.Count != list.Count)
          return false;
        for (int index = 0; index < this.Count; ++index)
        {
          if (!this[index].IsEquals(list[index]))
            return false;
        }
        return true;
      }

      public void Prepare()
      {
        for (int index = 0; index < this.Count; ++index)
          this[index].Prepare();
      }
    }

    internal class GroupColumnList : List<SelectStatement.ResultColumn>
    {
      private SelectStatement parent;

      public GroupColumnList(SelectStatement parent)
      {
        this.parent = parent;
      }

      public new int IndexOf(SelectStatement.ResultColumn column)
      {
        for (int index = 0; index < this.Count; ++index)
        {
          if (this[index] == column)
            return index;
        }
        return -1;
      }

      public int Add(SelectStatement.ResultColumn column, int lineNo, int symbolNo)
      {
        if (base.IndexOf(column) >= 0)
          throw new VistaDBSQLException(568, column.Alias, lineNo, symbolNo);
        this.Add(column);
        return this.Count - 1;
      }

      public bool IsEquals(SelectStatement.GroupColumnList list)
      {
        if (this.Count != list.Count)
          return false;
        for (int index = 0; index < this.Count; ++index)
        {
          if (!this[index].IsEquals(list[index]))
            return false;
        }
        return true;
      }

      public void Prepare()
      {
        if (this.Count == 0)
          return;
        foreach (SelectStatement.ResultColumn resultColumn1 in (List<SelectStatement.ResultColumn>) this.parent.resultColumns)
        {
          string columnName = resultColumn1.ColumnName;
          if (!resultColumn1.Aggregate && !resultColumn1.IsExpression)
          {
            bool flag = false;
            foreach (SelectStatement.ResultColumn resultColumn2 in (List<SelectStatement.ResultColumn>) this)
            {
              if (resultColumn2.ColumnName != null && resultColumn2.ColumnName.CompareTo(columnName) == 0)
              {
                flag = true;
                break;
              }
            }
            if (!flag)
              throw new VistaDBSQLException(569, resultColumn1.Alias, resultColumn1.Signature.LineNo, resultColumn1.Signature.SymbolNo);
          }
        }
      }
    }

    internal class HavingClause
    {
      private Signature signature;
      private SelectStatement parent;

      public HavingClause(SelectStatement parent)
      {
        this.signature = (Signature) null;
        this.parent = parent;
      }

      public bool Evaluate()
      {
        if (!(this.signature == (Signature) null))
          return (bool) ((IValue) this.signature.Execute()).Value;
        return true;
      }

      public void Prepare()
      {
        if (this.signature == (Signature) null)
          return;
        if (this.signature.Prepare() == SignatureType.Constant && this.signature.SignatureType != SignatureType.Constant)
          this.signature = (Signature) ConstantSignature.CreateSignature(this.signature.Execute(), (Statement) this.parent);
        if (this.signature.DataType != VistaDBType.Bit)
          throw new VistaDBSQLException(570, "", this.signature.LineNo, this.signature.SymbolNo);
      }

      public bool IsEquals(SelectStatement.HavingClause clause)
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
            return !this.Evaluate();
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
            return this.Evaluate();
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
          this.signature = value;
          int columnCount1 = this.signature.ColumnCount;
          int columnCount2 = 0;
          if (columnCount1 > 0)
          {
            for (int index = 0; index < this.parent.groupColumns.Count; ++index)
            {
              if (!this.parent.groupColumns[index].Aggregate)
              {
                this.signature = this.signature.Relink(this.parent.groupColumns[index].Signature, ref columnCount2);
                if (columnCount2 == columnCount1)
                  break;
              }
            }
          }
          if (columnCount2 < columnCount1)
            throw new VistaDBSQLException(571, "", this.signature.LineNo, this.signature.SymbolNo);
          for (int index = 0; index < this.parent.aggregateExpressions.Count; ++index)
            this.signature = this.signature.Relink((Signature) this.parent.aggregateExpressions[index].Function, ref columnCount2);
        }
      }

      public bool GetIsChanged()
      {
        if (this.signature != (Signature) null)
          return this.signature.GetIsChanged();
        return false;
      }
    }

    internal class AggregateExpression
    {
      private AggregateFunction func;
      private string alias;

      public AggregateExpression(AggregateFunction func, string alias)
      {
        this.func = func;
        this.alias = alias;
      }

      public AggregateFunction Function
      {
        get
        {
          return this.func;
        }
      }

      public Signature Expression
      {
        get
        {
          return this.func.Expression;
        }
      }

      public string Alias
      {
        get
        {
          return this.alias;
        }
      }
    }

    internal class AggregateExpressionList : List<SelectStatement.AggregateExpression>
    {
      public int GetExprCount()
      {
        int num = 0;
        foreach (SelectStatement.AggregateExpression aggregateExpression in (List<SelectStatement.AggregateExpression>) this)
        {
          if (aggregateExpression.Expression != (Signature) null)
            ++num;
        }
        return num;
      }
    }
  }
}
