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
    protected AddRowMethod addRowMethod;
    private bool distinct;
    private TempTable resultTable;
    private ResultColumnList resultColumns;
    private GroupColumnList groupColumns;
    private HavingClause havingClause;
    private OrderColumnList orderColumns;
    private Signature topCountSig;
    private bool hasAggregate;
    private bool ordered;
    private bool unionAll;
    private SelectStatement unionQuery;
    private bool simpleAggregateAdded;
    private AggregateExpressionList aggregateExpressions;
    private HashGroupTable hashGroupTable;
    private TempTable groupTable;
    private bool distinctAggregate;
    private ResultColumnList nonAggColumns;
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
        return cacheFactory;
      }
    }

    internal int AggregateFunctionCount
    {
      get
      {
        if (aggregateExpressions == null)
          return 0;
        return aggregateExpressions.Count;
      }
    }

    public override IParameter DoGetParam(string paramName)
    {
      if (parent == null)
        return tempParam;
      return base.DoGetParam(paramName);
    }

    public override void DoSetReturnParameter(IParameter param)
    {
      if (parent == null)
        tempParam = param;
      else
        base.DoSetReturnParameter(param);
    }

    public override IParameter DoGetReturnParameter()
    {
      if (parent == null)
        return tempParam;
      return base.DoGetReturnParameter();
    }

    protected override void DoBeforeParse()
    {
      base.DoBeforeParse();
      orderColumns = new OrderColumnList(this);
      groupColumns = new GroupColumnList(this);
      resultColumns = new ResultColumnList(this);
      havingClause = new HavingClause(this);
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
        distinct = true;
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
          topCountSig = parser.NextSignature(false, true, -1);
          topCount = 0L;
          flag = false;
        }
        else if (parser.TokenValue.TokenType != TokenType.Integer)
          throw new VistaDBSQLException(507, "numeric integer value", parser.TokenValue.RowNo, parser.TokenValue.ColNo);
        if (topCountSig == (Signature) null)
          topCount = long.Parse(parser.TokenValue.Token);
        if (flag)
        {
          parser.SkipToken(true);
          parser.ExpectedExpression(")");
        }
        if (topCount < 0L)
          topCount = 0L;
        parser.SkipToken(true);
      }
      ParseSelectList(parser);
      if (ParseFromClause(parser))
      {
        ParseWhereClause(parser);
        ParseGroupByClause(parser);
        PrepareAggregateFunctions();
        ParseHavingClause(parser);
        ParseUnionOperator(parser);
        ParseOrderByClause(parser);
      }
      else
      {
        ParseWhereClause(parser);
        PrepareAggregateFunctions();
        ParseUnionOperator(parser);
      }
    }

    internal bool AddRow(DataRowType dataRowType, object list, bool throughUnion)
    {
      if (hasAggregate && !throughUnion)
        return AddAggregateRow();
      if (addRowMethod != null)
      {
        if (!addRowMethod(dataRowType, list, true))
          return false;
      }
      else
      {
        int visibleColumnCount = resultColumns.VisibleColumnCount;
        switch (dataRowType)
        {
          case DataRowType.ResultColumnList:
                        ResultColumnList resultColumnList = (ResultColumnList) list;
            resultTable.Insert();
            for (int index = 0; index < visibleColumnCount; ++index)
              resultTable.PutColumn(resultColumnList[index].Signature.Result, index);
            resultTable.Post();
            break;
          case DataRowType.ArrayList:
            ArrayList arrayList = (ArrayList) list;
            resultTable.Insert();
            for (int index = 0; index < visibleColumnCount; ++index)
              resultTable.PutColumn((IColumn) arrayList[index], index);
            resultTable.Post();
            break;
          case DataRowType.TableRow:
            Row row = (Row) list;
            resultTable.Insert();
            resultTable.CurrentRow = row;
            resultTable.Post();
            break;
        }
      }
      ++affectedRows;
      if (throughUnion || GetTopCount() > affectedRows)
        return true;
      if (ordered)
        return unionQuery == null;
      return false;
    }

    private bool EndOfStatement(SQLParser parser)
    {
      if (parser.EndOfText || parser.IsToken(";"))
        return true;
      if (parent is IFStatement)
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
            assignValueStatement = true;
            signature = parser.NextSignature(false, true, 6);
          }
        }
        else if (assignValueStatement)
          throw new VistaDBSQLException(642, (string) null, lineNo, symbolNo);
        if (signature == (Signature) null)
          signature = parser.NextSignature(false, true, 6);
        string str;
        if (parser.IsToken(",") || parser.IsToken("FROM") || (parser.IsToken("WHERE") || parser.IsToken("UNION")) || EndOfStatement(parser))
        {
          str = resultColumns.GenerateColumnAlias(signature);
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
                ResultColumn column = new ResultColumn(signature, str, false, paramName);
        hasAggregate = hasAggregate || column.Aggregate;
        resultColumns.Add(column);
        flag = true;
      }
      while (parser.IsToken(","));
    }

    private void ParseOrderByClause(SQLParser parser)
    {
      if (addRowMethod != null || !parser.IsToken("ORDER"))
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
                        ResultColumn resultColumn = resultColumns.AddOrderColumn(tableName, objectName, parser);
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
        orderColumns.Add(new OrderColumn(orderColumns, token, isOrdinal, ordinal, objectName, tableName, rowNo, colNo, orderDirection));
      }
      while (parser.IsToken(","));
      ordered = true;
    }

    private void ParseGroupByClause(SQLParser parser)
    {
      if (!parser.IsToken("GROUP"))
      {
        if (!hasAggregate)
          return;
        for (int index = 0; index < resultColumns.Count; ++index)
        {
                    ResultColumn resultColumn = resultColumns[index];
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
        lineNo = parser.TokenValue.RowNo;
        symbolNo = parser.TokenValue.ColNo;
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
                    ResultColumn column = resultColumns[signature];
          if (column == null)
          {
            string columnAlias = resultColumns.GenerateColumnAlias(signature);
            column = new ResultColumn(signature, columnAlias, true, (string) null);
            resultColumns.Add(column);
          }
          groupColumns.Add(column, signature.LineNo, signature.SymbolNo);
        }
        while (parser.IsToken(","));
        if (hasAggregate)
          return;
        distinct = true;
      }
    }

    private void ParseHavingClause(SQLParser parser)
    {
      if (!parser.IsToken("HAVING"))
        return;
      if (!hasAggregate)
        throw new VistaDBSQLException(575, "", parser.TokenValue.RowNo, parser.TokenValue.ColNo);
      havingClause.Signature = parser.NextSignature(true, true, 6);
    }

    private void PrepareAggregateFunctions()
    {
      if (!hasAggregate)
        return;
      List<AggregateFunction> list = new List<AggregateFunction>();
      distinctAggregate = false;
      aggregateExpressions = new AggregateExpressionList();
      for (int index = 0; index < resultColumns.VisibleColumnCount; ++index)
      {
                ResultColumn resultColumn = resultColumns[index];
        if (resultColumn.Aggregate)
        {
          resultColumn.Signature.GetAggregateFunctions(list);
          foreach (AggregateFunction func in list)
            aggregateExpressions.Add(new AggregateExpression(func, resultColumns.GenerateColumnAlias((Signature) null)));
          if (resultColumn.HasDistinctAggregate)
            distinctAggregate = true;
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
        unionAll = true;
        parser.SkipToken(true);
      }
      parser.ExpectedExpression("SELECT");
      unionQuery = (SelectStatement) new SelectUnionStatement(connection, (Statement) this, parser);
    }

    public override void SetChanged()
    {
      resultColumns.SetChanged();
      whereClause.SetChanged();
      havingClause.SetChanged();
      if (join == null)
        return;
      join.SetUpdated();
    }

    public void ClearChanged()
    {
      resultColumns.ClearChanged();
      whereClause.ClearChanged();
      havingClause.ClearChanged();
      if (join == null)
        return;
      join.ClearUpdated();
    }

        private void FindReplaceableLookupTables(IRowSet currentRowset, Dictionary<string, SourceTable> candidates, bool leftmost)
    {
      if (currentRowset == null)
        return;
      Join join = currentRowset as Join;
      if (join != null)
      {
        FindReplaceableLookupTables(join.LeftRowSet, candidates, leftmost);
        FindReplaceableLookupTables(join.RightRowSet, candidates, false);
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
      if (join != null)
      {
        IViewList views = Database.EnumViews();
        IVistaDBTableNameCollection tableNames = Database.GetTableNames();
        cacheFactory = !(join is Join) ? (CacheFactory) null : new CacheFactory();
        int tableIndex = 0;
        join = join.PrepareTables(tableNames, views, sourceTables, false, ref tableIndex);
      }
      if (topCountSig != (Signature) null)
      {
        int num1 = (int) topCountSig.Prepare();
      }
      sourceTables.Prepare();
      PrepareOptimize();
      resultColumns.Prepare();
      if (assignValueStatement)
      {
        foreach (ResultColumn resultColumn in (List<ResultColumn>) resultColumns)
        {
          string paramName = resultColumn.ParamName;
          if (paramName != null && parent.DoGetParam(paramName) == null)
            throw new VistaDBSQLException(616, "@" + paramName, lineNo, symbolNo);
        }
      }
      whereClause.Prepare();
      orderColumns.Prepare();
      if (join != null)
        join.Prepare();
      groupColumns.Prepare();
      havingClause.Prepare();
      sourceTables.Unprepare();
      for (int index = 0; index < resultColumns.Count; ++index)
        resultColumns[index].Width = resultColumns[index].Signature.GetWidth();
      if (unionQuery != null)
      {
        int num2 = (int) unionQuery.PrepareQuery();
        if (resultColumns.VisibleColumnCount != unionQuery.resultColumns.VisibleColumnCount)
          throw new VistaDBSQLException(577, "", unionQuery.resultColumns[0].Signature.LineNo, unionQuery.resultColumns[0].Signature.SymbolNo);
        for (int index = 0; index < resultColumns.VisibleColumnCount; ++index)
        {
                    ResultColumn resultColumn = resultColumns[index];
          if (!Utils.CompatibleTypes(unionQuery.resultColumns[index].DataType, resultColumn.DataType))
            throw new VistaDBSQLException(578, "", unionQuery.resultColumns[0].Signature.LineNo, unionQuery.resultColumns[0].Signature.SymbolNo);
          int width = unionQuery.resultColumns[index].Width;
          if (width > resultColumns[index].Width)
            resultColumns[index].Width = width;
        }
      }
      if (resultColumns.Count - resultColumns.HiddenSortColumnCount == 1)
        return resultColumns[0].DataType;
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      if (assignValueStatement)
        SetChanged();
      SourceTable table;
      IQueryResult result = IsLiveQuery() ? ExecuteLiveQuery((Signature[]) null, true, out table) : ExecuteNonLiveQuery();
      if (!assignValueStatement)
        return result;
      CalculateParametersValue(result);
      return (IQueryResult) null;
    }

    protected override bool AcceptRow()
    {
      if (!hasAggregate)
        resultColumns.Execute();
      return AddRow(DataRowType.ResultColumnList, (object) resultColumns, false);
    }

    public override IQuerySchemaInfo GetSchemaInfo()
    {
      return (IQuerySchemaInfo) this;
    }

    private long GetTopCount()
    {
      if (topCountSig != (Signature) null && topCount == 0L)
      {
        IValue destinationValue = (IValue) new BigIntColumn();
        Database.Conversion.Convert((IValue) topCountSig.Execute(), destinationValue);
        topCount = (long) destinationValue.Value;
      }
      if (!singleRow)
        return topCount;
      return topCount <= 0L ? 0L : 1L;
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
          resultColumns.Execute();
          result.NextRow();
        }
        int index = 0;
        for (int columnCount = result.GetColumnCount(); index < columnCount; ++index)
        {
                    ResultColumn resultColumn = resultColumns[index];
          if (resultColumn.ParamName != null)
          {
            IParameter parameter = DoGetParam(resultColumn.ParamName);
            IColumn column = resultColumn.Signature.CreateColumn(parameter.DataType);
            Database.Conversion.Convert(IsLiveQuery() ? (IValue) resultColumn.Signature.Result : (IValue) result.GetColumn(index), (IValue) column);
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
      Optimize();
      simpleAggregateAdded = false;
      nonAggColumns = (ResultColumnList) null;
    }

        internal void SwitchToTemporaryTable(SourceRow sourceRow, int columnIndex, ResultColumn resultColumn)
    {
      if (whereClause == null || whereClause.Signature == (Signature) null)
        return;
      whereClause.Signature.SwitchToTempTable(sourceRow, columnIndex, resultColumn);
    }

    internal IQueryResult ExecuteLiveQuery(Signature[] signatures, bool readOnly, out SourceTable table)
    {
      table = (SourceTable) null;
      if (whereClause.IsAlwaysFalse)
      {
        endOfTable = true;
        return (IQueryResult) this;
      }
      PreExecute();
      if (signatures != null)
      {
        int index = 0;
        for (int length = signatures.Length; index < length; ++index)
        {
                    ResultColumn resultColumn = resultColumns[index];
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
      sourceTables.Open();
      FirstRow();
      return (IQueryResult) this;
    }

    internal IQueryResult ExecuteNonLiveQuery()
    {
      PreExecute();
      if (hasAggregate)
        CreateGroupTable();
      else
        CreateTempTable();
      affectedRows = 0L;
      try
      {
        bool flag = whereClause.IsAlwaysFalse;
        if (!flag)
        {
          if (sourceTables.Count > 0)
          {
            try
            {
              ExecuteJoin();
            }
            finally
            {
              Close();
            }
          }
          else
            AcceptRow();
          if (hasAggregate)
            GroupBy();
          else if ((unionQuery == null || !unionAll) && (distinct && addRowMethod == null))
            resultTable.Sort((QueryResultKey[]) null, true, false);
        }
        if (unionQuery != null)
        {
          unionQuery.ExecuteQuery();
          if (!unionAll && addRowMethod == null)
            resultTable.Sort((QueryResultKey[]) null, true, false);
          flag = resultTable != null && resultTable.RowCount > 0L;
        }
        if (flag)
          return (IQueryResult) resultTable;
        SetChanged();
        if (addRowMethod != null)
          return (IQueryResult) null;
        OrderBy();
        affectedRows = resultTable.RowCount;
        return (IQueryResult) resultTable;
      }
      finally
      {
        groupTable = (TempTable) null;
        hashGroupTable = (HashGroupTable) null;
        if (resultTable != null)
          resultTable.FirstRow();
        resultTable = (TempTable) null;
      }
    }

    private bool AddAggregateRow()
    {
      if (hashGroupTable != null)
      {
        hashGroupTable.AddRowToAggregateStream();
        return true;
      }
      if (resultTable == null)
      {
        if (simpleAggregateAdded)
        {
          for (int index = 0; index < aggregateExpressions.Count; ++index)
          {
            object newVal = (object) null;
            AggregateFunction function = aggregateExpressions[index].Function;
            if (function.Expression != (Signature) null)
              newVal = ((IValue) aggregateExpressions[index].Expression.Execute()).Value;
            if (!function.AddRowToGroup(newVal))
              return false;
          }
        }
        else
        {
          simpleAggregateAdded = true;
          for (int index = 0; index < aggregateExpressions.Count; ++index)
          {
            object newVal = (object) null;
            AggregateFunction function = aggregateExpressions[index].Function;
            if (function.Expression != (Signature) null)
              newVal = ((IValue) aggregateExpressions[index].Expression.Execute()).Value;
            function.CreateNewGroup(newVal);
          }
        }
        return true;
      }
      resultTable.Insert();
      for (int index = 0; index < nonAggColumns.Count; ++index)
        resultTable.PutColumn(nonAggColumns[index].Signature.Execute(), index);
      int count = nonAggColumns.Count;
      for (int index = 0; index < aggregateExpressions.Count; ++index)
      {
        Signature expression = aggregateExpressions[index].Expression;
        if (expression != (Signature) null)
        {
          resultTable.PutColumn(expression.Execute(), count);
          ++count;
        }
      }
      resultTable.Post();
      return true;
    }

    private void CreateTempTable()
    {
      resultTable = new TempTable(connection.Database);
      for (int index = 0; index < resultColumns.VisibleColumnCount; ++index)
        resultTable.AddColumn(resultColumns[index].Alias, resultColumns[index].DataType);
      resultTable.FinalizeCreate();
    }

    private void CreateGroupTable()
    {
      if (groupColumns.Count == 0 && !distinctAggregate)
        return;
      if (CanBeUsedHashGroupTable())
      {
        hashGroupTable = new HashGroupTable(Database, addRowMethod, resultColumns, groupColumns, aggregateExpressions, havingClause);
        resultTable = (TempTable) hashGroupTable;
      }
      else
      {
        resultTable = new TempTable(connection.Database);
        nonAggColumns = new ResultColumnList(this);
        for (int index = 0; index < resultColumns.Count; ++index)
        {
                    ResultColumn resultColumn = resultColumns[index];
          if (!resultColumn.Aggregate)
          {
            resultTable.AddColumn(resultColumn.Alias, resultColumn.Signature.DataType);
            nonAggColumns.Add(resultColumn);
          }
        }
        for (int index = 0; index < aggregateExpressions.Count; ++index)
        {
          Signature expression = aggregateExpressions[index].Expression;
          if (expression != (Signature) null)
            resultTable.AddColumn(aggregateExpressions[index].Alias, expression.DataType);
        }
        resultTable.FinalizeCreate();
        groupTable = resultTable;
        groupTable.Insert();
      }
    }

    private bool CanBeUsedHashGroupTable()
    {
      if (distinctAggregate || !connection.GetGroupOptimization())
        return false;
      int index = 0;
      for (int count = groupColumns.Count; index < count; ++index)
      {
        if (Utils.IsLongDataType(groupColumns[index].DataType))
          return false;
      }
      return true;
    }

    private TempTable CreateSecondGroupTable()
    {
      TempTable tempTable = new TempTable(connection.Database);
      for (int index = 0; index < aggregateExpressions.Count; ++index)
      {
                AggregateExpression aggregateExpression = aggregateExpressions[index];
        tempTable.AddColumn(aggregateExpression.Alias, aggregateExpression.Function.DataType);
      }
      for (int visibleColumnCount = resultColumns.VisibleColumnCount; visibleColumnCount < resultColumns.Count; ++visibleColumnCount)
      {
                ResultColumn resultColumn = resultColumns[visibleColumnCount];
        tempTable.AddColumn(resultColumn.Alias, resultColumn.Signature.DataType);
      }
      return tempTable;
    }

    private void OrderBy()
    {
      if (orderColumns.Count <= 0)
        return;
      QueryResultKey[] sortOrder = new QueryResultKey[orderColumns.Count];
      int index = 0;
      for (int count = orderColumns.Count; index < count; ++index)
      {
        sortOrder[index].ColumnIndex = orderColumns[index].ColumnIndex;
        sortOrder[index].Descending = orderColumns[index].OrderDirection == OrderDirection.Descending;
      }
      resultTable.Sort(sortOrder, false, false);
      if (unionQuery == null)
        resultTable.Truncate(GetTopCount());
      resultTable.FirstRow();
    }

    private void GroupBy()
    {
      if (hashGroupTable != null)
      {
        hashGroupTable.FinishAggregateStream();
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
        if (resultTable == null)
        {
          if (simpleAggregateAdded)
          {
            foreach (AggregateExpression aggregateExpression in (List<AggregateExpression>) aggregateExpressions)
              aggregateExpression.Function.FinishGroup();
          }
          else
          {
            foreach (AggregateExpression aggregateExpression in (List<AggregateExpression>) aggregateExpressions)
              aggregateExpression.Function.CreateEmptyResult();
          }
          int index1 = 0;
          for (int visibleColumnCount = resultColumns.VisibleColumnCount; index1 < visibleColumnCount; ++index1)
            resultColumns[index1].Signature.Execute();
          if (addRowMethod != null)
          {
            int num = addRowMethod(DataRowType.ResultColumnList, (object) resultColumns, true) ? 1 : 0;
          }
          else
          {
            CreateTempTable();
            resultTable.Insert();
            for (int index2 = 0; index2 < resultColumns.VisibleColumnCount; ++index2)
              resultTable.PutColumn(resultColumns[index2].Signature.Result, index2);
            resultTable.Post();
          }
        }
        else if (resultTable.RowCount == 0L)
        {
          groupTable.Close();
          if (addRowMethod != null)
            return;
          CreateTempTable();
        }
        else
        {
          for (int columnIndex = 0; columnIndex < nonAggColumns.Count; ++columnIndex)
            nonAggColumns[columnIndex].Signature.SwitchToTempTable(sourceRow1, columnIndex);
          int sortColumnCount = resultTable.CurrentRow.Count - aggregateExpressions.GetExprCount();
          QueryResultKey[] sortOrder;
          if (groupColumns.Count == 0)
          {
            sortOrder = (QueryResultKey[]) null;
          }
          else
          {
            sortOrder = new QueryResultKey[groupColumns.Count];
            int index = 0;
            for (int count = groupColumns.Count; index < count; ++index)
            {
              sortOrder[index].ColumnIndex = nonAggColumns.IndexOf(groupColumns[index]);
              sortOrder[index].Descending = false;
            }
          }
          bool manyGroups = sortOrder != null;
          for (int index = 0; index < aggregateExpressions.Count; ++index)
          {
            if (aggregateExpressions[index].Function.Distinct)
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
          if (addRowMethod != null && !distinctAggregate)
          {
            list = new ArrayList(resultColumns.VisibleColumnCount);
            for (int index = 0; index < resultColumns.VisibleColumnCount; ++index)
              list.Add((object) null);
          }
          CreateTempTable();
          TempTable secondTable = !distinctAggregate ? (TempTable) null : CreateSecondGroupTable();
          if (flag1)
          {
            if (manyGroups)
              groupTable.Sort(sortOrder, false, false);
            AggregateStream(secondTable, sourceRow1, -1, true, list, sortColumnCount, manyGroups, queryResultKeyArrayList == null);
            flag2 = true;
          }
          if (distinctAggregate)
          {
            int index1 = 0;
            for (int count = queryResultKeyArrayList.Count; index1 < count; ++index1)
            {
              groupTable.Sort(queryResultKeyArrayList[index1], false, true);
              AggregateStream(secondTable, sourceRow1, intList[index1], !flag2, list, sortColumnCount, manyGroups, index1 == queryResultKeyArrayList.Count - 1);
              flag2 = true;
            }
            for (int columnIndex = 0; columnIndex < resultColumns.VisibleColumnCount; ++columnIndex)
            {
              if (!resultColumns[columnIndex].Aggregate)
                resultColumns[columnIndex].Signature.SwitchToTempTable(sourceRow1, columnIndex);
            }
            int count1 = aggregateExpressions.Count;
            for (int visibleColumnCount = resultColumns.VisibleColumnCount; visibleColumnCount < resultColumns.Count; ++visibleColumnCount)
            {
              resultColumns[visibleColumnCount].Signature.SwitchToTempTable(sourceRow2, count1);
              ++count1;
            }
            for (int columnIndex = 0; columnIndex < aggregateExpressions.Count; ++columnIndex)
              aggregateExpressions[columnIndex].Function.SwitchToTempTable(sourceRow2, columnIndex);
            resultTable.FirstRow();
            secondTable.FirstRow();
            while (!resultTable.EndOfTable)
            {
              sourceRow1.Row = (IRow) resultTable.CurrentRow;
              sourceRow2.Row = (IRow) secondTable.CurrentRow;
              if (havingClause.Evaluate())
              {
                for (int index2 = 0; index2 < resultColumns.VisibleColumnCount; ++index2)
                {
                                    ResultColumn resultColumn = resultColumns[index2];
                  if (resultColumn.Aggregate)
                    ((IValue) sourceRow1.Row[index2]).Value = ((IValue) resultColumn.Signature.Execute()).Value;
                }
                if (addRowMethod != null)
                {
                  int num = addRowMethod(DataRowType.TableRow, (object) sourceRow1.Row, true) ? 1 : 0;
                }
                resultTable.NextRow();
              }
              else
              {
                if (addRowMethod == null)
                  resultTable.Delete();
                else
                  resultTable.NextRow();
                --affectedRows;
              }
              secondTable.NextRow();
            }
            secondTable.Close();
            if (addRowMethod != null)
              resultTable.Close();
          }
          for (int index = 0; index < nonAggColumns.Count; ++index)
            nonAggColumns[index].Signature.SwitchToTable();
          for (int index = 0; index < aggregateExpressions.Count; ++index)
            aggregateExpressions[index].Function.SwitchToTable();
          groupTable.Close();
        }
      }
    }

    private void AggregateStream(TempTable secondTable, SourceRow sourceRow, int functionIndex, bool insertNew, ArrayList list, int sortColumnCount, bool manyGroups, bool lastGrouping)
    {
      bool flag1 = false;
      IVistaDBRow vistaDbRow1 = (IVistaDBRow) null;
      IVistaDBRow vistaDbRow2 = (IVistaDBRow) null;
      bool insertIntoTemp = addRowMethod == null || list == null;
      bool flag2 = functionIndex >= 0;
      affectedRows = 0L;
      groupTable.FirstRow();
      if (!insertNew)
      {
        if (distinctAggregate)
          secondTable.FirstRow();
        else
          resultTable.FirstRow();
      }
      while (true)
      {
        bool flag3 = groupTable.EndOfTable;
        if (!flag3)
        {
          if (flag1)
          {
            if (manyGroups)
            {
              vistaDbRow2 = (IVistaDBRow) groupTable.GetCurrentKeyClone();
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
            vistaDbRow1 = manyGroups ? (IVistaDBRow) groupTable.GetCurrentKeyClone() : (IVistaDBRow) null;
            sourceRow.Row = groupTable.GetCurrentRowClone();
          }
        }
        if (flag3 || !flag1)
        {
          if (flag1)
          {
            if (AddRowFromAggregateStream(secondTable, functionIndex, insertIntoTemp, insertNew, list, sourceRow))
            {
              if (!groupTable.EndOfTable)
              {
                vistaDbRow1 = vistaDbRow2;
                sourceRow.Row = groupTable.GetCurrentRowClone();
              }
            }
            else
              break;
          }
          else
            flag1 = true;
          CreateNewAggregateGroup(functionIndex, sortColumnCount);
        }
        else
          AddRowToAggregateGroup(functionIndex, sortColumnCount);
        if (!groupTable.EndOfTable)
          groupTable.NextRow();
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
        aggregateExpressions[functionIndex].Function.FinishGroup();
      }
      else
      {
        foreach (AggregateExpression aggregateExpression in (List<AggregateExpression>) aggregateExpressions)
        {
          if (!aggregateExpression.Function.Distinct)
            aggregateExpression.Function.FinishGroup();
        }
      }
      if (!distinctAggregate && !havingClause.Evaluate())
        return true;
      if (insertIntoTemp && insertNew)
      {
        resultTable.Insert();
        if (distinctAggregate)
          secondTable.Insert();
      }
      int index1 = 0;
      if (insertNew || !distinctAggregate)
      {
        for (int index2 = 0; index2 < resultColumns.VisibleColumnCount; ++index2)
        {
                    ResultColumn resultColumn = resultColumns[index2];
          if (!resultColumn.Aggregate)
          {
            if (insertNew)
            {
              if (insertIntoTemp)
                resultTable.PutColumn(sourceRow.Row[index1], index2);
              else
                list[index2] = (object) sourceRow.Row[index1];
              ++index1;
            }
          }
          else if (!distinctAggregate)
          {
            if (insertIntoTemp)
              resultTable.PutColumn(resultColumn.Signature.Execute(), index2);
            else
              list[index2] = (object) resultColumn.Signature.Execute();
          }
        }
      }
      if (distinctAggregate)
      {
        for (int index2 = 0; index2 < aggregateExpressions.Count; ++index2)
        {
          AggregateFunction function = aggregateExpressions[index2].Function;
          if (functionIndex < 0 && !function.Distinct || functionIndex == index2)
            secondTable.PutColumn(function.Result, index2);
        }
        int count = aggregateExpressions.Count;
        for (int visibleColumnCount = resultColumns.VisibleColumnCount; visibleColumnCount < resultColumns.Count; ++visibleColumnCount)
        {
                    ResultColumn resultColumn = resultColumns[visibleColumnCount];
          secondTable.PutColumn(resultColumn.Signature.Execute(), count);
          ++count;
        }
        secondTable.Post();
        if (!insertNew)
          secondTable.NextRow();
        else
          resultTable.Post();
      }
      else if (insertIntoTemp)
      {
        resultTable.Post();
        if (!insertNew)
          resultTable.NextRow();
      }
      else
      {
        int num = addRowMethod(DataRowType.ArrayList, (object) list, true) ? 1 : 0;
      }
      ++affectedRows;
      if (GetTopCount() > affectedRows)
        return true;
      if (ordered)
        return unionQuery == null;
      return false;
    }

    private void CreateNewAggregateGroup(int functionIndex, int sortColumnCount)
    {
      int index1 = sortColumnCount;
      for (int index2 = 0; index2 < aggregateExpressions.Count; ++index2)
      {
        AggregateFunction function = aggregateExpressions[index2].Function;
        if (function.Expression == (Signature) null)
        {
          if (functionIndex < 0 && !function.Distinct || functionIndex == index2)
            function.CreateNewGroup((object) null);
        }
        else
        {
          if (functionIndex < 0 && !function.Distinct || functionIndex == index2)
          {
            object newVal = ((IValue) groupTable.GetColumn(index1)).Value;
            function.CreateNewGroup(newVal);
          }
          ++index1;
        }
      }
    }

    private void AddRowToAggregateGroup(int functionIndex, int sortColumnCount)
    {
      int index1 = sortColumnCount;
      for (int index2 = 0; index2 < aggregateExpressions.Count; ++index2)
      {
        AggregateFunction function = aggregateExpressions[index2].Function;
        if (function.Expression == (Signature) null)
        {
          if (functionIndex < 0 && !function.Distinct || functionIndex == index2)
            function.AddRowToGroup((object) null);
        }
        else
        {
          if (functionIndex < 0 && !function.Distinct || functionIndex == index2)
          {
            object newVal = ((IValue) groupTable.GetColumn(index1)).Value;
            function.AddRowToGroup(newVal);
          }
          ++index1;
        }
      }
    }

    public bool IsEquals(SelectStatement statement)
    {
      if (distinct == statement.distinct && resultColumns.IsEquals(statement.resultColumns) && (join.IsEquals(statement.join) && whereClause.IsEquals(statement.whereClause)) && (groupColumns.IsEquals(statement.groupColumns) && havingClause.IsEquals(statement.havingClause)))
        return orderColumns.IsEquals(statement.orderColumns);
      return false;
    }

    public bool GetIsChanged()
    {
      if (!resultColumns.GetIsChanged() && (join == null || !join.RowUpdated) && !whereClause.GetIsChanged())
        return havingClause.GetIsChanged();
      return true;
    }

    public bool IsLiveQuery()
    {
      if (join != null && !hasAggregate && (orderColumns.Count == 0 && unionQuery == null) && addRowMethod == null)
        return !distinct;
      return false;
    }

    public void FreeTables()
    {
      sourceTables.Free();
      resultColumns.SetChanged();
      whereClause.SetChanged();
      havingClause.SetChanged();
      if (join == null)
        return;
      join.SetUpdated();
    }

    public int ResultColumnCount
    {
      get
      {
        return resultColumns.Count;
      }
    }

    public bool UnionAll
    {
      get
      {
        return unionAll;
      }
    }

    public SelectStatement UnionQuery
    {
      get
      {
        return unionQuery;
      }
    }

    public bool HasAggregate
    {
      get
      {
        return hasAggregate;
      }
    }

    public bool Distinct
    {
      get
      {
        return distinct;
      }
    }

    internal bool IsSetTopCount
    {
      get
      {
        if (topCount == long.MaxValue)
          return topCountSig != (Signature) null;
        return true;
      }
    }

    public bool Sorted
    {
      get
      {
        return orderColumns.Count > 0;
      }
    }

    public bool HasWhereClause
    {
      get
      {
        return whereClause.Signature != (Signature) null;
      }
    }

    public bool HasOrderByClause
    {
      get
      {
        if (orderColumns != null)
          return orderColumns.Count > 0;
        return false;
      }
    }

    public bool HasHavingClause
    {
      get
      {
        return havingClause.Signature != (Signature) null;
      }
    }

    public bool HasGroupByClause
    {
      get
      {
        if (groupColumns != null)
          return groupColumns.Count > 0;
        return false;
      }
    }

    public string GetAliasName(int ordinal)
    {
      return resultColumns[ordinal].Alias;
    }

    public int GetColumnOrdinal(string name)
    {
      return resultColumns.IndexOf(name);
    }

    public int GetWidth(int ordinal)
    {
      return resultColumns[ordinal].Width;
    }

    public bool GetIsKey(int ordinal)
    {
      return resultColumns[ordinal].IsKey;
    }

    public string GetColumnName(int ordinal)
    {
      return resultColumns[ordinal].ColumnName;
    }

    public string GetTableName(int ordinal)
    {
      return resultColumns[ordinal].TableName;
    }

    public Type GetColumnType(int ordinal)
    {
      return resultColumns[ordinal].SystemType;
    }

    public bool GetIsAllowNull(int ordinal)
    {
      return resultColumns[ordinal].IsAllowNull;
    }

    public VistaDBType GetColumnVistaDBType(int ordinal)
    {
      return resultColumns[ordinal].DataType;
    }

    public bool GetIsAliased(int ordinal)
    {
      return resultColumns[ordinal].IsAliased;
    }

    public bool GetIsExpression(int ordinal)
    {
      return resultColumns[ordinal].IsExpression;
    }

    public bool GetIsAutoIncrement(int ordinal)
    {
      return resultColumns[ordinal].IsAutoIncrement;
    }

    public bool GetIsLong(int ordinal)
    {
      return resultColumns[ordinal].IsLong;
    }

    public bool GetIsReadOnly(int ordinal)
    {
      return resultColumns[ordinal].IsReadOnly;
    }

    public string GetDataTypeName(int ordinal)
    {
      return resultColumns[ordinal].DataType.ToString();
    }

    public DataTable GetSchemaTable()
    {
      if (!prepared)
        return (DataTable) null;
      DataTable schemaTableInstance = GetSchemaTableInstance();
      schemaTableInstance.BeginLoadData();
      int index = 0;
      for (int columnCount = ColumnCount; index < columnCount; ++index)
      {
                ResultColumn resultColumn = resultColumns[index];
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
      return resultColumns[ordinal].Description;
    }

    public string GetColumnCaption(int ordinal)
    {
      return resultColumns[ordinal].Caption;
    }

    public bool GetIsEncrypted(int ordinal)
    {
      return resultColumns[ordinal].Encrypted;
    }

    public int GetCodePage(int ordinal)
    {
      return resultColumns[ordinal].CodePage;
    }

    public string GetIdentity(int ordinal, out string step, out string seed)
    {
            ResultColumn resultColumn = resultColumns[ordinal];
      step = resultColumn.IdentityStep;
      seed = resultColumn.IdentitySeed;
      return resultColumn.Identity;
    }

    public string GetDefaultValue(int ordinal, out bool useInUpdate)
    {
            ResultColumn resultColumn = resultColumns[ordinal];
      useInUpdate = resultColumn.UseInUpdate;
      return resultColumn.DefaultValue;
    }

    public int ColumnCount
    {
      get
      {
        return resultColumns.VisibleColumnCount - resultColumns.HiddenSortColumnCount;
      }
    }

    public void FirstRow()
    {
      if (sourceTables.Count == 0 || GetTopCount() == 0L)
      {
        endOfTable = true;
        affectedRows = 0L;
      }
      else
      {
        if (!sourceTables.AllOpen && sourceTables.HasNative)
          sourceTables.Open();
        endOfTable = !sourceTables[0].First(constraintOperations) || !AcceptJoinedRow();
        affectedRows = endOfTable ? 0L : 1L;
      }
    }

    public void NextRow()
    {
      if (sourceTables.Count == 0 || GetTopCount() <= affectedRows)
      {
        endOfTable = true;
      }
      else
      {
        endOfTable = !join.Next(constraintOperations) || !AcceptJoinedRow();
        if (endOfTable)
          return;
        ++affectedRows;
      }
    }

    public void Close()
    {
      if (connection != null)
      {
        if (cacheFactory != null)
          cacheFactory.Close();
        sourceTables.Free();
      }
      SetChanged();
      endOfTable = true;
    }

    public object GetValue(int index, VistaDBType dataType)
    {
      if (endOfTable)
        return (object) null;
      IColumn column1 = resultColumns[index].Signature.Execute();
      if (dataType == VistaDBType.Unknown || column1.InternalType == dataType)
        return ((IValue) column1).Value;
      IColumn column2 = resultColumns[index].Signature.CreateColumn(dataType);
      Database.Conversion.Convert((IValue) column1, (IValue) column2);
      return ((IValue) column2).Value;
    }

    public IColumn GetColumn(int index)
    {
      if (!endOfTable)
        return resultColumns[index].Signature.Execute();
      return (IColumn) null;
    }

    public bool IsNull(int index)
    {
      return resultColumns[index].Signature.Execute().IsNull;
    }

    public int GetColumnCount()
    {
      return resultColumns.VisibleColumnCount;
    }

    public bool EndOfTable
    {
      get
      {
        return endOfTable;
      }
    }

    public long RowCount
    {
      get
      {
        return affectedRows;
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
        aggregate = signature.HasAggregateFunction(out aggregateDistinct);
        width = 0;
        columnName = (string) null;
        tableName = (string) null;
        isKey = false;
        isAllowNull = true;
        isExpression = true;
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
        this.paramName = paramName;
      }

      public ResultColumn(ColumnSignature column, string alias)
        : this((Signature) column, alias, false, (string) null)
      {
        columnName = column.ColumnName;
        tableName = column.Table.TableName;
        tableAlias = column.Table.Alias;
        isKey = column.IsKey;
        isAllowNull = column.IsAllowNull;
        isExpression = column.IsExpression;
        isAutoIncrement = column.IsAutoIncrement;
        isReadOnly = column.IsReadOnly;
        description = column.Description;
        caption = column.Caption;
        encrypted = column.Encrypted;
        codePage = column.CodePage;
        identity = column.Identity;
        identityStep = column.IdentityStep;
        identitySeed = column.IdentitySeed;
        defaultValue = column.DefaultValue;
        useInUpdate = column.UseInUpdate;
      }

      public bool Prepare()
      {
        SignatureType signatureType;
        try
        {
          signatureType = signature.Prepare();
        }
        catch (Exception ex)
        {
          throw new VistaDBSQLException(ex, 662, alias, signature.LineNo, signature.SymbolNo);
        }
        switch (signatureType)
        {
          case SignatureType.Constant:
            if (signature.SignatureType != SignatureType.Constant)
            {
              signature = (Signature) ConstantSignature.CreateSignature(signature.Execute(), signature.Parent);
              break;
            }
            goto default;
          case SignatureType.MultiplyColumn:
            return false;
          default:
            if (signature.DataType == VistaDBType.Unknown)
              throw new VistaDBSQLException(561, alias, signature.LineNo, signature.SymbolNo);
            break;
        }
        isAllowNull = signature.IsAllowNull;
        if (signature.SignatureType == SignatureType.Column || signature.SignatureType == SignatureType.ExternalColumn)
        {
          ColumnSignature signature = (ColumnSignature) this.signature;
          if (!signature.IsExpression)
          {
            columnName = signature.ColumnName;
            tableName = signature.Table.TableName;
            tableAlias = signature.Table.Alias;
            isKey = signature.IsKey;
            isAllowNull = signature.IsAllowNull;
            isExpression = signature.IsExpression;
            isAutoIncrement = signature.IsAutoIncrement;
            isReadOnly = signature.IsReadOnly;
            description = signature.Description;
            caption = signature.Caption;
            encrypted = signature.Encrypted;
            codePage = signature.CodePage;
            identity = signature.Identity;
            identityStep = signature.IdentityStep;
            identitySeed = signature.IdentitySeed;
            defaultValue = signature.DefaultValue;
            useInUpdate = signature.UseInUpdate;
          }
        }
        return true;
      }

      public Signature Signature
      {
        get
        {
          return signature;
        }
      }

      public string Alias
      {
        get
        {
          return alias;
        }
      }

      public bool Hidden
      {
        get
        {
          return hidden;
        }
        set
        {
          hidden = value;
        }
      }

      public bool Aggregate
      {
        get
        {
          return aggregate;
        }
      }

      public bool HasDistinctAggregate
      {
        get
        {
          return aggregateDistinct;
        }
      }

      public VistaDBType DataType
      {
        get
        {
          return signature.DataType;
        }
      }

      public int Width
      {
        get
        {
          return width;
        }
        set
        {
          width = value;
        }
      }

      public string ColumnName
      {
        get
        {
          return columnName;
        }
      }

      public string TableName
      {
        get
        {
          return tableName;
        }
      }

      public string TableAlias
      {
        get
        {
          return tableAlias;
        }
      }

      public bool IsKey
      {
        get
        {
          return isKey;
        }
      }

      public bool IsAllowNull
      {
        get
        {
          return isAllowNull;
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

      public string ParamName
      {
        get
        {
          return paramName;
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

      public bool IsAliased
      {
        get
        {
          if (!isExpression)
            return signature.Parent.Connection.CompareString(alias, columnName, true) != 0;
          return true;
        }
      }

      public Type SystemType
      {
        get
        {
          return Utils.GetSystemType(DataType);
        }
      }

      public bool IsLong
      {
        get
        {
          switch (DataType)
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

      public bool IsEquals(ResultColumn column)
      {
        if (DataType == column.DataType)
          return signature == column.signature;
        return false;
      }

      public bool GetIsChanged()
      {
        return signature.GetIsChanged();
      }

      internal void ReplaceSignature(QuickJoinLookupColumn newColumnSignature)
      {
        if (!((Signature) newColumnSignature != (Signature) null))
          return;
        signature = (Signature) newColumnSignature;
      }
    }

    internal class ResultColumnList : List<ResultColumn>
    {
      private SelectStatement parent;
      private int visibleColumnCount;
      private int hiddenSortColumnCount;

      public ResultColumnList(SelectStatement parent)
      {
        this.parent = parent;
        visibleColumnCount = 0;
        hiddenSortColumnCount = 0;
      }

      public new int Add(ResultColumn column)
      {
        if (!column.Hidden)
          ++visibleColumnCount;
        base.Add(column);
        return Count - 1;
      }

      public new void Insert(int index, ResultColumn column)
      {
        if (!column.Hidden)
          ++visibleColumnCount;
        base.Insert(index, column);
      }

      public int IndexOf(string alias)
      {
        for (int index = 0; index < Count; ++index)
        {
          if (parent.Connection.CompareString(alias, base[index].Alias, true) == 0)
            return index;
        }
        return -1;
      }

      public int IndexOf(Signature signature)
      {
        for (int index = 0; index < Count; ++index)
        {
          if (base[index].Signature == signature)
            return index;
        }
        return -1;
      }

      public ResultColumn AddOrderColumn(string tableName, string columnName, SQLParser parser)
      {
                ResultColumn column1 = (ResultColumn) null;
        int index1 = -1;
        for (int index2 = 0; index2 < Count; ++index2)
        {
          column1 = base[index2];
          if (tableName == null && parent.Connection.CompareString(columnName, column1.Alias, true) == 0)
          {
            index1 = index2;
            break;
          }
          Signature signature = column1.Signature;
          ColumnSignature columnSignature;
          if (signature.SignatureType == SignatureType.Column || signature.SignatureType == SignatureType.ExternalColumn)
          {
            columnSignature = (ColumnSignature) signature;
            if ((tableName == null || parent.Connection.CompareString(tableName, column1.TableName, true) == 0) && parent.Connection.CompareString(columnName, column1.Alias, true) == 0)
            {
              index1 = index2;
              break;
            }
          }
          else if (signature.SignatureType == SignatureType.MultiplyColumn)
          {
            columnSignature = (ColumnSignature) signature;
            if (tableName == null || column1.TableName == null || parent.Connection.CompareString(tableName, column1.TableName, true) == 0)
            {
              index1 = index2;
              break;
            }
          }
        }
        if (index1 >= 0)
        {
          if (index1 > visibleColumnCount)
          {
            column1 = this[index1];
            column1.Hidden = false;
            RemoveAt(index1);
            Insert(visibleColumnCount, column1);
          }
          return column1;
        }
        ColumnSignature signature1 = ColumnSignature.CreateSignature(tableName, columnName, parser);
        string columnAlias = GenerateColumnAlias((Signature) signature1);
                ResultColumn column2 = new ResultColumn((Signature) signature1, columnAlias, false, (string) null);
        Insert(visibleColumnCount, column2);
        ++hiddenSortColumnCount;
        return column2;
      }

      public bool IsEquals(ResultColumnList list)
      {
        if (Count != list.Count)
          return false;
        for (int index = 0; index < Count; ++index)
        {
          if (!this[index].IsEquals(list[index]))
            return false;
        }
        return true;
      }

      public void Execute()
      {
        for (int index = 0; index < Count; ++index)
          this[index].Signature.Execute();
      }

      public void Prepare()
      {
        List<ColumnSignature> columnSignatureList = new List<ColumnSignature>();
        int index1 = 0;
        for (int count1 = Count; index1 < count1; ++index1)
        {
                    ResultColumn resultColumn = this[index1];
          if (!resultColumn.Prepare())
          {
            --visibleColumnCount;
            ((ColumnSignature) resultColumn.Signature).ExtractColumns(columnSignatureList);
            RemoveAt(index1);
            int count2 = columnSignatureList.Count;
            for (int index2 = 0; index2 < count2; ++index2)
            {
              ColumnSignature column = columnSignatureList[index2];
              string columnAlias = GenerateColumnAlias((Signature) column);
              Insert(index1 + index2, new ResultColumn(column, columnAlias));
            }
            int num = count2 - 1;
            count1 += num;
            index1 += num;
            columnSignatureList.Clear();
          }
        }
        if (!parent.hasAggregate)
          return;
        for (int index2 = 0; index2 < visibleColumnCount; ++index2)
        {
                    ResultColumn resultColumn1 = this[index2];
          if (resultColumn1.Aggregate)
          {
            int columnCount1 = resultColumn1.Signature.ColumnCount;
            int columnCount2 = 0;
            foreach (ResultColumn resultColumn2 in (List<ResultColumn>) this)
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
        for (; IndexOf(alias) >= 0; alias = str + "_" + num.ToString())
          ++num;
        return alias;
      }

      public ResultColumn GetColumnByComplexName(string tableName, string columnName)
      {
        foreach (ResultColumn resultColumn in (List<ResultColumn>) this)
        {
          if (parent.connection.CompareString(resultColumn.TableAlias, tableName, true) == 0 && parent.connection.CompareString(resultColumn.ColumnName, columnName, true) == 0 || parent.connection.CompareString(resultColumn.TableName, tableName, true) == 0 && parent.connection.CompareString(resultColumn.ColumnName, columnName, true) == 0 || (parent.connection.CompareString(resultColumn.Alias, columnName, true) == 0 && (string.IsNullOrEmpty(resultColumn.TableName) || parent.connection.CompareString(resultColumn.TableName, tableName, true) == 0) || parent.connection.CompareString(resultColumn.Alias, columnName, true) == 0 && parent.connection.CompareString(resultColumn.TableAlias, tableName, true) == 0))
            return resultColumn;
        }
        return (ResultColumn) null;
      }

      public void SetChanged()
      {
        for (int index = 0; index < Count; ++index)
          this[index].Signature.SetChanged();
      }

      public void ClearChanged()
      {
        for (int index = 0; index < Count; ++index)
          this[index].Signature.ClearChanged();
      }

      public new ResultColumn this[int index]
      {
        get
        {
          if (index < 0 || index >= Count)
            return (ResultColumn) null;
          return base[index];
        }
      }

      public ResultColumn this[string alias]
      {
        get
        {
          int index = IndexOf(alias);
          if (index < 0)
            return (ResultColumn) null;
          return base[index];
        }
      }

      public ResultColumn this[Signature signature]
      {
        get
        {
          int index = IndexOf(signature);
          if (index < 0)
            return (ResultColumn) null;
          return base[index];
        }
      }

      public bool GetIsChanged()
      {
        for (int index = 0; index < Count; ++index)
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
          return visibleColumnCount;
        }
      }

      public int HiddenSortColumnCount
      {
        get
        {
          return hiddenSortColumnCount;
        }
      }
    }

    private class OrderColumn
    {
      private OrderColumnList parent;
      private ResultColumn column;
      private OrderDirection orderDirection;
      private int lineNo;
      private int symbolNo;
      private bool isOrdinal;
      private int ordinal;
      private string columnName;
      private string tableName;
      private string text;
      private int columnIndex;

      public OrderColumn(OrderColumnList parent, string text, bool isOrdinal, int ordinal, string columnName, string tableName, int lineNo, int symbolNo, OrderDirection orderDirection)
      {
        this.parent = parent;
        this.lineNo = lineNo;
        this.symbolNo = symbolNo;
        this.orderDirection = orderDirection;
        column = (ResultColumn) null;
        this.text = text;
        this.isOrdinal = isOrdinal;
        this.ordinal = ordinal;
        this.columnName = columnName;
        this.tableName = tableName;
        columnIndex = -1;
      }

      public bool IsEquals(OrderColumn column)
      {
        if (this.column == null || column.column == null)
        {
          if (isOrdinal == column.isOrdinal && ordinal == column.ordinal && (parent.Parent.Connection.CompareString(columnName, column.columnName, true) == 0 && parent.Parent.Connection.CompareString(tableName, column.tableName, true) == 0))
            return orderDirection == column.orderDirection;
          return false;
        }
        if (this.column.IsEquals(column.column))
          return orderDirection == column.orderDirection;
        return false;
      }

      public void Prepare()
      {
        column = !isOrdinal ? (tableName != null ? parent.Parent.resultColumns.GetColumnByComplexName(tableName, columnName) : parent.Parent.resultColumns[columnName]) : parent.Parent.resultColumns[ordinal - 1];
        if (column == null)
          throw new VistaDBSQLException(567, text, lineNo, symbolNo);
        columnIndex = parent.Parent.resultColumns.IndexOf(column);
      }

      public ResultColumn Column
      {
        get
        {
          return column;
        }
      }

      public OrderDirection OrderDirection
      {
        get
        {
          return orderDirection;
        }
      }

      public int ColumnIndex
      {
        get
        {
          return columnIndex;
        }
      }
    }

    private class OrderColumnList : List<OrderColumn>
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
          return parent;
        }
      }

      public bool IsEquals(OrderColumnList list)
      {
        if (Count != list.Count)
          return false;
        for (int index = 0; index < Count; ++index)
        {
          if (!this[index].IsEquals(list[index]))
            return false;
        }
        return true;
      }

      public void Prepare()
      {
        for (int index = 0; index < Count; ++index)
          this[index].Prepare();
      }
    }

    internal class GroupColumnList : List<ResultColumn>
    {
      private SelectStatement parent;

      public GroupColumnList(SelectStatement parent)
      {
        this.parent = parent;
      }

      public new int IndexOf(ResultColumn column)
      {
        for (int index = 0; index < Count; ++index)
        {
          if (this[index] == column)
            return index;
        }
        return -1;
      }

      public int Add(ResultColumn column, int lineNo, int symbolNo)
      {
        if (base.IndexOf(column) >= 0)
          throw new VistaDBSQLException(568, column.Alias, lineNo, symbolNo);
        Add(column);
        return Count - 1;
      }

      public bool IsEquals(GroupColumnList list)
      {
        if (Count != list.Count)
          return false;
        for (int index = 0; index < Count; ++index)
        {
          if (!this[index].IsEquals(list[index]))
            return false;
        }
        return true;
      }

      public void Prepare()
      {
        if (Count == 0)
          return;
        foreach (ResultColumn resultColumn1 in (List<ResultColumn>) parent.resultColumns)
        {
          string columnName = resultColumn1.ColumnName;
          if (!resultColumn1.Aggregate && !resultColumn1.IsExpression)
          {
            bool flag = false;
            foreach (ResultColumn resultColumn2 in (List<ResultColumn>) this)
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
        signature = (Signature) null;
        this.parent = parent;
      }

      public bool Evaluate()
      {
        if (!(signature == (Signature) null))
          return (bool) ((IValue) signature.Execute()).Value;
        return true;
      }

      public void Prepare()
      {
        if (signature == (Signature) null)
          return;
        if (signature.Prepare() == SignatureType.Constant && signature.SignatureType != SignatureType.Constant)
          signature = (Signature) ConstantSignature.CreateSignature(signature.Execute(), (Statement) parent);
        if (signature.DataType != VistaDBType.Bit)
          throw new VistaDBSQLException(570, "", signature.LineNo, signature.SymbolNo);
      }

      public bool IsEquals(HavingClause clause)
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
            return !Evaluate();
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
            return Evaluate();
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
          signature = value;
          int columnCount1 = signature.ColumnCount;
          int columnCount2 = 0;
          if (columnCount1 > 0)
          {
            for (int index = 0; index < parent.groupColumns.Count; ++index)
            {
              if (!parent.groupColumns[index].Aggregate)
              {
                signature = signature.Relink(parent.groupColumns[index].Signature, ref columnCount2);
                if (columnCount2 == columnCount1)
                  break;
              }
            }
          }
          if (columnCount2 < columnCount1)
            throw new VistaDBSQLException(571, "", signature.LineNo, signature.SymbolNo);
          for (int index = 0; index < parent.aggregateExpressions.Count; ++index)
            signature = signature.Relink((Signature) parent.aggregateExpressions[index].Function, ref columnCount2);
        }
      }

      public bool GetIsChanged()
      {
        if (signature != (Signature) null)
          return signature.GetIsChanged();
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
          return func;
        }
      }

      public Signature Expression
      {
        get
        {
          return func.Expression;
        }
      }

      public string Alias
      {
        get
        {
          return alias;
        }
      }
    }

    internal class AggregateExpressionList : List<AggregateExpression>
    {
      public int GetExprCount()
      {
        int num = 0;
        foreach (AggregateExpression aggregateExpression in (List<AggregateExpression>) this)
        {
          if (aggregateExpression.Expression != (Signature) null)
            ++num;
        }
        return num;
      }
    }
  }
}
