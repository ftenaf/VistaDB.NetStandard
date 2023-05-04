using System.Collections;
using System.Collections.Generic;
using VistaDB.DDA;
using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
    internal class ConstraintOperations : Stack
    {
        private OptimizationParts constraints = new OptimizationParts();
        private OptimizationLevel optimizationLevel;
        private IDatabase database;
        private TableCollection sourceTables;

        internal ConstraintOperations(IDatabase db, TableCollection sourceTables)
          : base(100)
        {
            database = db;
            this.sourceTables = sourceTables;
            optimizationLevel = OptimizationLevel.Full;
        }

        public override int Count
        {
            get
            {
                return constraints.Count;
            }
        }

        internal OptimizationLevel OptimizationLevel
        {
            get
            {
                return optimizationLevel;
            }
        }

        internal void ResetFullOptimizationLevel()
        {
            if (optimizationLevel != OptimizationLevel.Full)
                return;
            optimizationLevel = OptimizationLevel.Part;
        }

        new private Constraint Pop()
        {
            return (Constraint)base.Pop();
        }

        private bool AddJoinConstraint(ColumnSignature leftColumn, ColumnSignature rightColumn, CompareOperation cmp, CompareOperation revCmp)
        {
            if (cmp == CompareOperation.Equal && leftColumn.DataType == rightColumn.DataType && leftColumn.Table.CollectionOrder != rightColumn.Table.CollectionOrder)
            {
                constraints.Add(leftColumn.Table.CollectionOrder <= rightColumn.Table.CollectionOrder ? new JoinColumnEqualityConstraint(rightColumn, leftColumn) : (Constraint)new JoinColumnEqualityConstraint(leftColumn, rightColumn));
                return true;
            }
            if (leftColumn.Table.CollectionOrder > rightColumn.Table.CollectionOrder)
                return AddValueConstraint(leftColumn, rightColumn, cmp, false, true);
            return AddValueConstraint(rightColumn, leftColumn, revCmp, false, true);
        }

        private bool AddNullValueConstraint(ColumnSignature column, bool isNull)
        {
            constraints.Add(new IsNullConstraint(column, isNull));
            return true;
        }

        private bool AddValueConstraint(ColumnSignature column, Signature valueSignature, CompareOperation cmp, bool fts, bool join)
        {
            Signature leftConstantValue = null;
            Signature rightConstantValue = null;
            switch (cmp)
            {
                case CompareOperation.Equal:
                case CompareOperation.NotEqual:
                    leftConstantValue = valueSignature;
                    rightConstantValue = valueSignature;
                    break;
                case CompareOperation.Greater:
                case CompareOperation.GreaterOrEqual:
                    leftConstantValue = valueSignature;
                    break;
                case CompareOperation.Less:
                case CompareOperation.LessOrEqual:
                    rightConstantValue = valueSignature;
                    break;
            }
            constraints.Add(join ? new JoinColumnCompareConstraint(column, leftConstantValue, rightConstantValue, cmp) : (Constraint)new ColumnCompareConstraint(column, leftConstantValue, rightConstantValue, cmp, fts));
            return true;
        }

        private bool AddScopeValueContraint(ColumnSignature column, Signature low, Signature high, bool fts)
        {
            constraints.Add(new ColumnCompareConstraint(column, low, high, CompareOperation.InScope, fts));
            return true;
        }

        private bool AddConstantConstraint(Signature leftOperand, Signature rightOperand, CompareOperation cmp)
        {
            constraints.Add(new ConstantsCompareConstraint(leftOperand, rightOperand, cmp));
            return true;
        }

        internal bool AnalyzeOptimizationLevel()
        {
            if (constraints.Count == 0)
                return false;
            foreach (Constraint constraint in (List<Constraint>)constraints)
            {
                constraint.Optimized = false;
                constraint.FullOptimized = false;
                if (!constraint.IsBundle)
                    constraint.Analyze();
            }
            Clear();
            foreach (Constraint constraint1 in (List<Constraint>)constraints)
            {
                switch (constraint1.Type)
                {
                    case ConstraintType.And:
                        Constraint constraint2 = Pop();
                        Constraint constraint3 = Pop();
                        constraint1.Optimized = constraint2.Optimized || constraint3.Optimized;
                        constraint1.FullOptimized = constraint2.FullOptimized && constraint3.FullOptimized;
                        Push(constraint1);
                        continue;
                    case ConstraintType.Or:
                        Constraint constraint4 = Pop();
                        Constraint constraint5 = Pop();
                        constraint1.Optimized = constraint4.Optimized && constraint5.Optimized;
                        constraint1.FullOptimized = constraint4.FullOptimized && constraint5.FullOptimized;
                        Push(constraint1);
                        continue;
                    case ConstraintType.Not:
                        Constraint constraint6 = Pop();
                        bool flag = constraint6.Optimized && constraint6.FullOptimized;
                        constraint1.Optimized = flag;
                        constraint1.FullOptimized = flag;
                        Push(constraint1);
                        continue;
                    default:
                        Push(constraint1);
                        continue;
                }
            }
            Constraint constraint7 = Pop();
            if (!constraint7.Optimized)
                optimizationLevel = OptimizationLevel.None;
            else if (!constraint7.FullOptimized)
                optimizationLevel = OptimizationLevel.Part;
            return optimizationLevel != OptimizationLevel.None;
        }

        internal bool ActivateOptimizedFilter(int tableOrder)
        {
            if (optimizationLevel == OptimizationLevel.None)
                return false;
            Clear();
            bool emptyResultSet;
            if (sourceTables[tableOrder].ActivateOptimizedConstraints(out emptyResultSet))
            {
                if (optimizationLevel == OptimizationLevel.Full)
                    optimizationLevel = OptimizationLevel.Part;
                return emptyResultSet;
            }
            bool resetFullOptimization = false;
            foreach (Constraint constraint1 in (List<Constraint>)constraints)
            {
                switch (constraint1.Type)
                {
                    case ConstraintType.And:
                        Constraint left1 = Pop();
                        Constraint right1 = Pop();
                        constraint1.Conjunction(database, tableOrder, left1, right1, out resetFullOptimization);
                        Push(constraint1);
                        break;
                    case ConstraintType.Or:
                        Constraint left2 = Pop();
                        Constraint right2 = Pop();
                        constraint1.Disjunction(database, tableOrder, left2, right2, out resetFullOptimization);
                        Push(constraint1);
                        break;
                    case ConstraintType.Not:
                        Constraint constraint2 = Pop();
                        constraint2.Invertion();
                        Push(constraint2);
                        break;
                    default:
                        constraint1.InitializeBuilding(database, tableOrder, sourceTables);
                        Push(constraint1);
                        break;
                }
                if (resetFullOptimization && optimizationLevel == OptimizationLevel.Full)
                    optimizationLevel = OptimizationLevel.Part;
            }
            Constraint constraint = Pop();
            try
            {
                return constraint.ActivateFilter(tableOrder, out resetFullOptimization);
            }
            finally
            {
                if (resetFullOptimization && optimizationLevel == OptimizationLevel.Full)
                    optimizationLevel = OptimizationLevel.Part;
            }
        }

        internal bool AddLogicalBetween(ColumnSignature column, Signature low, Signature high, bool fts)
        {
            if (column.SignatureType != SignatureType.MultiplyColumn && low.SignatureType != SignatureType.Column && high.SignatureType != SignatureType.Column)
                return AddScopeValueContraint(column, low, high, fts);
            return false;
        }

        internal bool AddLogicalIsNull(Signature columnOperand, bool isNull)
        {
            if (columnOperand.SignatureType == SignatureType.Column)
                return AddNullValueConstraint((ColumnSignature)columnOperand, isNull);
            return false;
        }

        internal bool AddLogicalCompare(Signature leftOperand, Signature rightOperand, CompareOperation cmp, CompareOperation revCmp, bool fts)
        {
            if (leftOperand.SignatureType == SignatureType.Column && rightOperand.SignatureType == SignatureType.Column)
                return AddJoinConstraint((ColumnSignature)leftOperand, (ColumnSignature)rightOperand, cmp, revCmp);
            if (leftOperand.SignatureType == SignatureType.Column)
                return AddValueConstraint((ColumnSignature)leftOperand, rightOperand, cmp, fts, false);
            if (rightOperand.SignatureType == SignatureType.Column)
                return AddValueConstraint((ColumnSignature)rightOperand, leftOperand, revCmp, false, false);
            if ((leftOperand.SignatureType == SignatureType.Parameter || leftOperand.SignatureType == SignatureType.Constant) && (rightOperand.SignatureType == SignatureType.Parameter || rightOperand.SignatureType == SignatureType.Constant))
                return AddConstantConstraint(leftOperand, rightOperand, cmp);
            return false;
        }

        internal bool AddLogicalExpression(Signature operand)
        {
            return false;
        }

        internal bool AddLogicalNot()
        {
            constraints.Add(new NotBundle());
            return true;
        }

        internal bool AddLogicalAnd()
        {
            constraints.Add(new AndBundle());
            return true;
        }

        internal bool AddLogicalOr()
        {
            constraints.Add(new OrBundle());
            return true;
        }

        internal bool RollBackAddedConstraints(int oldCount)
        {
            int count = Count - oldCount;
            if (count <= 0)
                return false;
            if (oldCount <= 0)
            {
                ClearConstraints();
                return true;
            }
            constraints.RemoveRange(oldCount, count);
            return true;
        }

        internal void ClearConstraints()
        {
            constraints.Clear();
            Clear();
        }

        internal string GetIndexName(int rowIndex, int tableOrder)
        {
            if (optimizationLevel == OptimizationLevel.None)
                return null;
            foreach (Constraint constraint in (List<Constraint>)constraints)
            {
                if (constraint.Type == ConstraintType.Bitwise)
                {
                    string optimizedIndexName = constraint.GetOptimizedIndexName(tableOrder);
                    if (optimizedIndexName != null)
                        return optimizedIndexName;
                }
            }
            return null;
        }

        internal string GetJoinedTable(int orOrder, SourceTable table)
        {
            if (optimizationLevel == OptimizationLevel.None)
                return null;
            foreach (Constraint constraint in (List<Constraint>)constraints)
            {
                if (constraint.Type == ConstraintType.Bitwise)
                {
                    string joinedTable = constraint.GetJoinedTable(table);
                    if (joinedTable != null)
                        return joinedTable;
                }
            }
            return null;
        }

        private enum ConstraintType
        {
            Bitwise,
            And,
            Or,
            Not,
        }

        private class OptimizationParts : List<Constraint>
        {
            internal OptimizationParts()
              : base(20)
            {
            }
        }

        private class AccumulatedResults : Dictionary<int, AccumulatedResults.OptimizationInfo>
        {
            private int keyColumnOrder = -1;
            private SourceTable table;
            private IVistaDBIndexInformation activeIndex;
            private bool descending;

            internal new OptimizationInfo this[int tableOrder]
            {
                get
                {
                    if (!ContainsKey(tableOrder))
                        return null;
                    return base[tableOrder];
                }
            }

            internal OptimizationInfo.ScopeInfo EvaluatedScope
            {
                get
                {
                    return this[table.CollectionOrder].GetScopeInfo(activeIndex);
                }
            }

            internal IVistaDBIndexInformation OptimizationIndex
            {
                get
                {
                    return activeIndex;
                }
            }

            internal int KeyColumnOrder
            {
                get
                {
                    return keyColumnOrder;
                }
            }

            internal bool Descending
            {
                get
                {
                    return descending;
                }
            }

            internal void InitOptimization(SourceTable table, IVistaDBIndexInformation index, int keyColumnOrder, bool descending)
            {
                this.table = table;
                activeIndex = index;
                this.keyColumnOrder = keyColumnOrder;
                this.descending = descending;
            }

            internal void InitTableResult(IRow leftScope, IRow rightScope)
            {
                Add(table.CollectionOrder, new OptimizationInfo(table, activeIndex, leftScope, rightScope));
            }

            internal void AddTableResult(OptimizationInfo resultInfo)
            {
                Add(resultInfo.Table.CollectionOrder, resultInfo);
            }

            internal void CopyFrom(AccumulatedResults accumulatedResults)
            {
                Clear();
                foreach (OptimizationInfo resultInfo in accumulatedResults.Values)
                    AddTableResult(resultInfo);
            }

            internal string OptimizationIndexByTableOrder(int tableOrder)
            {
                if (table == null || table.CollectionOrder != tableOrder)
                    return null;
                if (activeIndex != null)
                    return activeIndex.Name;
                return null;
            }

            internal class OptimizationInfo
            {
                private ScopeInfoCollection scopes = new ScopeInfoCollection();
                private SourceTable table;
                private IOptimizedFilter filter;
                private IVistaDBIndexInformation filterIndex;

                internal OptimizationInfo(SourceTable table, IVistaDBIndexInformation index, IRow leftScope, IRow rightScope)
                {
                    this.table = table;
                    filterIndex = index;
                    scopes.AddScope(index, leftScope, rightScope);
                }

                internal SourceTable Table
                {
                    get
                    {
                        return table;
                    }
                }

                internal IOptimizedFilter Filter
                {
                    get
                    {
                        return filter;
                    }
                }

                internal bool ShouldBeMerged
                {
                    get
                    {
                        if (scopes.Count > 1)
                            return true;
                        if (scopes.Count == 1)
                            return filter != null;
                        return false;
                    }
                }

                internal ScopeInfo GetScopeInfo(IVistaDBIndexInformation index)
                {
                    return scopes[index.Name];
                }

                internal IRow GetLeftScope(IVistaDBIndexInformation index)
                {
                    return scopes[index.Name]?.LeftScope;
                }

                internal IRow GetRightScope(IVistaDBIndexInformation index)
                {
                    return scopes[index.Name]?.RightScope;
                }

                private void ConvertToBitmapAndConjunction(string scopeIndexName, Constraint parentConstraint)
                {
                    ScopeInfo scope = scopes[scopeIndexName];
                    filterIndex = scope.Index;
                    IOptimizedFilter filter = table.BuildFilterMap(scopeIndexName, scope.LeftScope, scope.RightScope, parentConstraint.ExcludeNulls);
                    if (this.filter == null)
                        this.filter = filter;
                    else
                        this.filter.Conjunction(filter);
                    scopes.Remove(scopeIndexName);
                }

                private void ConvertToBitmapAndDisjunction(string scopeIndexName, Constraint parentConstraint)
                {
                    ScopeInfo scope = scopes[scopeIndexName];
                    filterIndex = scope.Index;
                    IOptimizedFilter filter = table.BuildFilterMap(scopeIndexName, scope.LeftScope, scope.RightScope, parentConstraint.ExcludeNulls);
                    if (this.filter == null)
                        this.filter = filter;
                    else
                        this.filter.Disjunction(filter);
                    scopes.Remove(scopeIndexName);
                }

                internal OptimizationInfo Disjunction(IDatabase db, OptimizationInfo rightInfo, Constraint leftParent, Constraint rightParent)
                {
                    foreach (string key in (IEnumerable)((Hashtable)scopes.Clone()).Keys)
                    {
                        ScopeInfo scope = rightInfo.scopes[key];
                        if (scope == null)
                        {
                            ConvertToBitmapAndDisjunction(key, leftParent);
                        }
                        else
                        {
                            bool emptyResult;
                            if (!scopes[key].Disjunction(scope, out emptyResult))
                            {
                                rightInfo.ConvertToBitmapAndDisjunction(key, rightParent);
                                ConvertToBitmapAndDisjunction(key, leftParent);
                            }
                            else if (emptyResult)
                                return null;
                        }
                    }
                    foreach (string key in (IEnumerable)((Hashtable)rightInfo.scopes.Clone()).Keys)
                    {
                        if (scopes[key] == null)
                            rightInfo.ConvertToBitmapAndDisjunction(key, rightParent);
                    }
                    if (filter != null)
                    {
                        filter.Disjunction(rightInfo.filter);
                        filterIndex = rightInfo.filterIndex;
                    }
                    return this;
                }

                internal OptimizationInfo Conjunction(IDatabase db, OptimizationInfo rightInfo, Constraint leftParent, Constraint rightParent)
                {
                    foreach (string key in (IEnumerable)((Hashtable)scopes.Clone()).Keys)
                    {
                        ScopeInfo scope = rightInfo.scopes[key];
                        if (scope != null)
                        {
                            bool emptyResult;
                            if (!scopes[key].Conjunction(scope, out emptyResult))
                            {
                                rightInfo.ConvertToBitmapAndConjunction(key, rightParent);
                                ConvertToBitmapAndConjunction(key, leftParent);
                            }
                            else if (emptyResult)
                                return null;
                        }
                    }
                    foreach (string key in (IEnumerable)rightInfo.scopes.Keys)
                    {
                        if (scopes[key] == null)
                            scopes.AddScope(rightInfo.scopes[key]);
                    }
                    if (filter != null && rightInfo.filter != null)
                    {
                        filter.Conjunction(rightInfo.filter);
                        filterIndex = rightInfo.filterIndex;
                    }
                    else if (rightInfo.filter != null)
                    {
                        filter = rightInfo.filter;
                        filterIndex = rightInfo.filterIndex;
                    }
                    return this;
                }

                internal void SimplifyBeforeDisjunction(Constraint parentConstraint, bool invert)
                {
                    foreach (string key in (IEnumerable)((Hashtable)scopes.Clone()).Keys)
                        ConvertToBitmapAndConjunction(key, parentConstraint);
                    if (!invert)
                        return;
                    filter.Invert(true);
                }

                internal IVistaDBIndexInformation SimplifyConjunction(Constraint parentConstraint, bool forceFinalBitmapAndInvert)
                {
                    int num = 0;
                    IVistaDBIndexInformation indexInformation = null;
                    foreach (string key in (IEnumerable)((Hashtable)scopes.Clone()).Keys)
                    {
                        if (num++ == 0)
                            indexInformation = scopes[key].Index;
                        else
                            ConvertToBitmapAndConjunction(key, parentConstraint);
                    }
                    if (forceFinalBitmapAndInvert)
                    {
                        if (indexInformation != null)
                            ConvertToBitmapAndConjunction(indexInformation.Name, parentConstraint);
                        filter.Invert(true);
                    }
                    return indexInformation ?? filterIndex;
                }

                internal void FinalizeBitmap(Constraint parentConstraint)
                {
                    foreach (string key in (IEnumerable)((Hashtable)scopes.Clone()).Keys)
                        ConvertToBitmapAndConjunction(key, parentConstraint);
                }

                private class ScopeInfoCollection : InsensitiveHashtable
                {
                    internal ScopeInfoCollection()
                    {
                    }

                    internal ScopeInfo this[string indexName]
                    {
                        get
                        {
                            if (!Contains(indexName))
                                return null;
                            return (ScopeInfo)this[(object)indexName];
                        }
                    }

                    internal void AddScope(IVistaDBIndexInformation scopeIndex, IRow leftScope, IRow rightScope)
                    {
                        Add(scopeIndex.Name, new ScopeInfo(scopeIndex, leftScope, rightScope));
                    }

                    internal void AddScope(ScopeInfo scopeInfo)
                    {
                        Add(scopeInfo.Index.Name, scopeInfo);
                    }
                }

                internal class ScopeInfo
                {
                    private IVistaDBIndexInformation index;
                    private IRow leftScope;
                    private IRow rightScope;

                    internal ScopeInfo(IVistaDBIndexInformation index, IRow leftScope, IRow rightScope)
                    {
                        this.index = index;
                        this.leftScope = leftScope;
                        this.rightScope = rightScope;
                    }

                    internal IVistaDBIndexInformation Index
                    {
                        get
                        {
                            return index;
                        }
                    }

                    internal IRow LeftScope
                    {
                        get
                        {
                            return leftScope;
                        }
                    }

                    internal IRow RightScope
                    {
                        get
                        {
                            return rightScope;
                        }
                    }

                    private IRow LessOf(IRow firstScope, IRow secondScope)
                    {
                        if (secondScope.Compare(firstScope) <= 0)
                            return secondScope;
                        return firstScope;
                    }

                    private IRow GreatestOf(IRow firstScope, IRow secondScope)
                    {
                        if (secondScope.Compare(firstScope) >= 0)
                            return secondScope;
                        return firstScope;
                    }

                    private bool IsIntersection(ScopeInfo right)
                    {
                        IRow leftScope1 = leftScope;
                        IRow rightScope = this.rightScope;
                        IRow leftScope2 = right.leftScope;
                        return right.rightScope.CompareKey(leftScope1) >= 0 && rightScope.CompareKey(leftScope2) >= 0;
                    }

                    internal bool Conjunction(ScopeInfo right, out bool emptyResult)
                    {
                        if (!IsIntersection(right))
                        {
                            emptyResult = false;
                            return false;
                        }
                        IRow row1 = GreatestOf(leftScope, right.leftScope);
                        IRow row2 = LessOf(rightScope, right.rightScope);
                        leftScope = row1;
                        rightScope = row2;
                        emptyResult = leftScope.CompareKey(rightScope) > 0;
                        return true;
                    }

                    internal bool Disjunction(ScopeInfo right, out bool emptyResult)
                    {
                        if (!IsIntersection(right))
                        {
                            emptyResult = false;
                            return false;
                        }
                        IRow row1 = LessOf(leftScope, right.leftScope);
                        IRow row2 = GreatestOf(rightScope, right.rightScope);
                        leftScope = row1;
                        rightScope = row2;
                        emptyResult = leftScope.CompareKey(rightScope) > 0;
                        return true;
                    }
                }
            }

            private class BitmapFilters : Hashtable
            {
            }
        }

        private class Constraint
        {
            protected Triangular.Value optimizedResult = Triangular.Value.Undefined;
            protected AccumulatedResults results = new AccumulatedResults();
            private ConstraintType type;
            private ColumnSignature column;
            protected Signature leftValue;
            protected Signature rightValue;
            private bool inverted;
            private bool originInverted;
            protected bool alwaysNull;
            protected CompareOperation compareOperation;
            private bool useFtsIndex;
            private bool optimized;
            private bool fullOptimized;

            protected Constraint(ConstraintType type)
            {
                compareOperation = CompareOperation.Equal;
                this.type = type;
            }

            protected Constraint(ConstraintType type, ColumnSignature column, Signature leftValue, Signature rightValue, CompareOperation compareOpration, bool fts)
              : this(type)
            {
                this.column = column;
                this.leftValue = leftValue;
                this.rightValue = rightValue;
                compareOperation = compareOpration;
                useFtsIndex = fts;
                if (!(column != null))
                    return;
                switch (compareOpration)
                {
                    case CompareOperation.NotEqual:
                        inverted = true;
                        originInverted = true;
                        compareOperation = CompareOperation.Equal;
                        break;
                    case CompareOperation.Greater:
                        if (!(rightValue == null))
                            break;
                        inverted = true;
                        originInverted = true;
                        compareOperation = CompareOperation.LessOrEqual;
                        this.rightValue = leftValue;
                        this.leftValue = null;
                        break;
                    case CompareOperation.Less:
                        if (!(leftValue == null))
                            break;
                        inverted = true;
                        originInverted = true;
                        compareOperation = CompareOperation.GreaterOrEqual;
                        this.leftValue = rightValue;
                        this.rightValue = null;
                        break;
                }
            }

            internal bool Optimized
            {
                get
                {
                    return optimized;
                }
                set
                {
                    optimized = value;
                }
            }

            internal bool FullOptimized
            {
                get
                {
                    return fullOptimized;
                }
                set
                {
                    fullOptimized = value;
                }
            }

            internal ConstraintType Type
            {
                get
                {
                    return type;
                }
            }

            internal bool IsBundle
            {
                get
                {
                    return type != ConstraintType.Bitwise;
                }
            }

            internal bool IsOrBundle
            {
                get
                {
                    return type == ConstraintType.Or;
                }
            }

            protected ColumnSignature ColumnSignature
            {
                get
                {
                    return column;
                }
            }

            internal bool IsAlwaysNull
            {
                get
                {
                    return alwaysNull;
                }
            }

            protected virtual bool NullValuesExcluded
            {
                get
                {
                    return false;
                }
            }

            internal virtual bool ExcludeNulls
            {
                get
                {
                    return true;
                }
            }

            internal bool ShouldBeMerged
            {
                get
                {
                    if (inverted)
                        return true;
                    foreach (AccumulatedResults.OptimizationInfo optimizationInfo in results.Values)
                    {
                        if (optimizationInfo.ShouldBeMerged)
                            return true;
                    }
                    return false;
                }
            }

            private bool TestIfEvaluable(ColumnSignature signature, TableCollection sourceTables, int currentTableOrder)
            {
                SourceTable table = signature.Table;
                if (table == null || !sourceTables.Contains(table) || table.CollectionOrder < currentTableOrder)
                    return true;
                SetOptimizableResult(Triangular.Value.True);
                return false;
            }

            protected void AddIndex(IVistaDBIndexInformation index, int keyColumnOrder, bool descending)
            {
                if (results.Count > 0)
                    return;
                results.InitOptimization(column.Table, index, keyColumnOrder, descending);
                optimized = true;
                fullOptimized = true;
            }

            private void EvalScope(IDatabase db, SourceTable table, int currentTableOrder, TableCollection sourceTables)
            {
                OnEvalScope(db, table, currentTableOrder, sourceTables);
            }

            internal void SetOptimizableResult(Triangular.Value value)
            {
                optimizedResult = value;
                fullOptimized = optimized = value != Triangular.Value.Undefined;
                results.Clear();
                inverted = false;
            }

            private void PropagateFrom(Constraint constraint)
            {
                column = constraint.column;
                results.CopyFrom(constraint.results);
                alwaysNull = constraint.alwaysNull;
                inverted = constraint.inverted;
                optimizedResult = constraint.optimizedResult;
            }

            protected virtual void OnEvalScope(IDatabase db, SourceTable table, int currentTableOrder, TableCollection sourceTables)
            {
                if (optimizedResult != Triangular.Value.Undefined)
                    return;
                if (table.CollectionOrder != currentTableOrder)
                {
                    SetOptimizableResult(Triangular.Value.True);
                }
                else
                {
                    ColumnSignature leftValue = this.leftValue as ColumnSignature;
                    ColumnSignature signature = ReferenceEquals(this.leftValue, rightValue) ? null : rightValue as ColumnSignature;
                    if (leftValue != null && !TestIfEvaluable(leftValue, sourceTables, currentTableOrder) || signature != null && !TestIfEvaluable(signature, sourceTables, currentTableOrder))
                        return;
                    bool descending = results.Descending;
                    IRow indexStructure = column.Table.DoGetIndexStructure(results.OptimizationIndex.Name);
                    IRow rightScope = indexStructure.CopyInstance();
                    indexStructure.InitTop();
                    rightScope.InitBottom();
                    indexStructure.RowId = Row.MinRowId + 1U;
                    rightScope.RowId = Row.MaxRowId - 1U;
                    if (compareOperation == CompareOperation.IsNull)
                    {
                        for (int index = 0; index < 1; ++index)
                        {
                            indexStructure[index].Value = null;
                            rightScope[index].Value = null;
                        }
                        results.InitTableResult(indexStructure, rightScope);
                    }
                    else
                    {
                        if (!descending && this.leftValue == null)
                            indexStructure.RowId = Row.MaxRowId;
                        if (descending && rightValue == null)
                            rightScope.RowId = Row.MinRowId;
                        IRow row1;
                        IRow row2;
                        if (descending)
                        {
                            row1 = rightScope;
                            row2 = indexStructure;
                        }
                        else
                        {
                            row1 = indexStructure;
                            row2 = rightScope;
                        }
                        int keyColumnOrder = results.KeyColumnOrder;
                        IColumn column = null;
                        if (this.leftValue != null)
                        {
                            column = this.leftValue.Execute();
                            if (column.IsNull && NullValuesExcluded)
                            {
                                SetOptimizableResult(Triangular.Value.False);
                                alwaysNull = true;
                                return;
                            }
                            if (results.OptimizationIndex.FullTextSearch)
                            {
                                row1[0].Value = (short)keyColumnOrder;
                                db.Conversion.Convert(column, row1[1]);
                            }
                            else
                                db.Conversion.Convert(column, row1[keyColumnOrder]);
                            if (compareOperation == CompareOperation.Greater)
                                row1.RowId = descending ? Row.MinRowId : Row.MaxRowId;
                        }
                        if (rightValue != null)
                        {
                            if (!ReferenceEquals(this.leftValue, rightValue))
                            {
                                column = rightValue.Execute();
                                if (column.IsNull && NullValuesExcluded)
                                {
                                    SetOptimizableResult(Triangular.Value.False);
                                    alwaysNull = true;
                                    return;
                                }
                            }
                            if (results.OptimizationIndex.FullTextSearch)
                            {
                                row2[0].Value = (short)keyColumnOrder;
                                db.Conversion.Convert(column, row2[1]);
                            }
                            else
                                db.Conversion.Convert(column, row2[keyColumnOrder]);
                            if (compareOperation == CompareOperation.Less)
                                row2.RowId = descending ? Row.MaxRowId : Row.MinRowId;
                        }
                        if (indexStructure.Compare(rightScope) > 0)
                            SetOptimizableResult(Triangular.Value.False);
                        results.InitTableResult(indexStructure, rightScope);
                    }
                }
            }

            protected virtual void OnInvertion()
            {
                if (IsAlwaysNull)
                    inverted = false;
                else
                    inverted = !inverted;
            }

            protected virtual void OnFindIndexes()
            {
                IVistaDBTableSchema tableSchema = column.Table.GetTableSchema();
                if (tableSchema == null)
                    return;
                foreach (IVistaDBIndexInformation index1 in (IEnumerable<IVistaDBIndexInformation>)tableSchema.Indexes.Values)
                {
                    if (!(useFtsIndex ^ index1.FullTextSearch))
                    {
                        int index2 = 0;
                        for (int index3 = index1.FullTextSearch ? index1.KeyStructure.Length : 1; index2 < index3; ++index2)
                        {
                            IVistaDBKeyColumn vistaDbKeyColumn = index1.KeyStructure[index2];
                            int rowIndex = vistaDbKeyColumn.RowIndex;
                            if (rowIndex == column.ColumnIndex)
                            {
                                int keyColumnOrder = index1.FullTextSearch ? rowIndex : index2;
                                AddIndex(index1, keyColumnOrder, vistaDbKeyColumn.Descending);
                                if (index1.KeyStructure.Length == 1)
                                    return;
                            }
                        }
                    }
                }
                if (results.OptimizationIndex != null)
                    return;
                IVistaDBIndexCollection temporaryIndexes = column.Table.TemporaryIndexes;
                if (temporaryIndexes == null)
                    return;
                foreach (IVistaDBIndexInformation index in (IEnumerable<IVistaDBIndexInformation>)temporaryIndexes)
                {
                    IVistaDBKeyColumn vistaDbKeyColumn = index.KeyStructure[0];
                    if (vistaDbKeyColumn.RowIndex == column.ColumnIndex)
                        AddIndex(index, 0, vistaDbKeyColumn.Descending);
                }
            }

            protected virtual void OnInitializeBuilding(IDatabase db, int currentTableOrder, TableCollection sourceTables)
            {
                EvalScope(db, column == null ? null : column.Table, currentTableOrder, sourceTables);
            }

            protected virtual void OnAnalyze()
            {
                FindIndexes();
            }

            internal void Analyze()
            {
                if (leftValue != null)
                    leftValue.SetChanged();
                if (rightValue != null)
                    rightValue.SetChanged();
                OnAnalyze();
            }

            internal string GetOptimizedIndexName(int tableOrder)
            {
                if (!optimized || results == null)
                    return null;
                return results.OptimizationIndexByTableOrder(tableOrder);
            }

            internal IVistaDBIndexInformation SimplifyConjunction(int tableOrder, out bool resetFullOptimization)
            {
                bool inverted = this.inverted;
                this.inverted = false;
                if (inverted && results.Count > 1)
                {
                    SetOptimizableResult(Triangular.Value.True);
                    resetFullOptimization = true;
                    return null;
                }
                resetFullOptimization = false;
                return results[tableOrder]?.SimplifyConjunction(this, inverted);
            }

            internal void SimplifyDisjunction(int tableOrder)
            {
                bool inverted = this.inverted;
                this.inverted = false;
                results[tableOrder]?.SimplifyBeforeDisjunction(this, inverted);
            }

            internal virtual void ActivateMustBeBitmap(int tableOrder)
            {
            }

            internal void FindIndexes()
            {
                OnFindIndexes();
            }

            internal void InitializeBuilding(IDatabase db, int currentTableOrder, TableCollection sourceTables)
            {
                results.Clear();
                optimizedResult = Triangular.Value.Undefined;
                inverted = originInverted;
                alwaysNull = false;
                OnInitializeBuilding(db, currentTableOrder, sourceTables);
            }

            internal void Invertion()
            {
                OnInvertion();
            }

            private bool OptimizeFullDisjunctionResult(IDatabase db, int currentTableOrder, Constraint left, Constraint right)
            {
                if (left.optimizedResult != Triangular.Value.Undefined && right.optimizedResult != Triangular.Value.Undefined)
                {
                    optimizedResult = Triangular.Or(left.optimizedResult, right.optimizedResult);
                    return true;
                }
                if (left.optimizedResult != Triangular.Value.Undefined)
                {
                    if (left.optimizedResult == Triangular.Value.True)
                    {
                        SetOptimizableResult(Triangular.Value.True);
                        return true;
                    }
                    PropagateFrom(right);
                    return true;
                }
                if (right.optimizedResult == Triangular.Value.Undefined)
                    return false;
                if (right.optimizedResult == Triangular.Value.True)
                {
                    SetOptimizableResult(Triangular.Value.True);
                    return true;
                }
                PropagateFrom(left);
                return true;
            }

            private bool OptimizeFullConjunctionResult(IDatabase db, int currentTableOrder, Constraint left, Constraint right)
            {
                if (left.optimizedResult != Triangular.Value.Undefined && right.optimizedResult != Triangular.Value.Undefined)
                {
                    SetOptimizableResult(Triangular.And(left.optimizedResult, right.optimizedResult));
                    return true;
                }
                if (left.optimizedResult != Triangular.Value.Undefined)
                {
                    if (left.optimizedResult == Triangular.Value.False)
                    {
                        SetOptimizableResult(Triangular.Value.False);
                        return true;
                    }
                    PropagateFrom(right);
                    return true;
                }
                if (right.optimizedResult == Triangular.Value.Undefined)
                    return false;
                if (right.optimizedResult == Triangular.Value.False)
                {
                    SetOptimizableResult(Triangular.Value.False);
                    return true;
                }
                PropagateFrom(left);
                return true;
            }

            internal void Disjunction(IDatabase db, int currentTableOrder, Constraint left, Constraint right, out bool resetFullOptimization)
            {
                SetOptimizableResult(Triangular.Value.Undefined);
                left.ActivateMustBeBitmap(currentTableOrder);
                right.ActivateMustBeBitmap(currentTableOrder);
                if (left.results.Count > 1 || right.results.Count > 1)
                {
                    SetOptimizableResult(Triangular.Value.False);
                    resetFullOptimization = true;
                }
                else
                {
                    resetFullOptimization = false;
                    if (left.ShouldBeMerged)
                        left.SimplifyDisjunction(currentTableOrder);
                    if (right.ShouldBeMerged)
                        right.SimplifyDisjunction(currentTableOrder);
                    if (OptimizeFullDisjunctionResult(db, currentTableOrder, left, right))
                        return;
                    foreach (AccumulatedResults.OptimizationInfo optimizationInfo in left.results.Values)
                    {
                        SourceTable table = optimizationInfo.Table;
                        if (table.CollectionOrder != currentTableOrder)
                        {
                            SetOptimizableResult(Triangular.Value.False);
                            break;
                        }
                        AccumulatedResults.OptimizationInfo result = right.results[table.CollectionOrder];
                        if (result != null)
                        {
                            AccumulatedResults.OptimizationInfo resultInfo = optimizationInfo.Disjunction(db, result, left, right);
                            if (resultInfo == null)
                            {
                                SetOptimizableResult(Triangular.Value.False);
                                break;
                            }
                            results.AddTableResult(resultInfo);
                        }
                    }
                }
            }

            internal void Conjunction(IDatabase db, int currentTableOrder, Constraint left, Constraint right, out bool resetFullOptimization)
            {
                SetOptimizableResult(Triangular.Value.Undefined);
                left.ActivateMustBeBitmap(currentTableOrder);
                right.ActivateMustBeBitmap(currentTableOrder);
                resetFullOptimization = false;
                if (left.inverted)
                    left.SimplifyConjunction(currentTableOrder, out resetFullOptimization);
                if (right.inverted)
                    right.SimplifyConjunction(currentTableOrder, out resetFullOptimization);
                if (OptimizeFullConjunctionResult(db, currentTableOrder, left, right))
                    return;
                results.Clear();
                foreach (AccumulatedResults.OptimizationInfo resultInfo1 in left.results.Values)
                {
                    SourceTable table = resultInfo1.Table;
                    if (table.CollectionOrder >= currentTableOrder)
                    {
                        AccumulatedResults.OptimizationInfo result = right.results[table.CollectionOrder];
                        if (result != null)
                        {
                            if (table.CollectionOrder == currentTableOrder)
                            {
                                AccumulatedResults.OptimizationInfo resultInfo2 = resultInfo1.Conjunction(db, result, left, right);
                                if (resultInfo2 == null)
                                {
                                    SetOptimizableResult(Triangular.Value.False);
                                    return;
                                }
                                results.AddTableResult(resultInfo2);
                            }
                            else
                                results.AddTableResult(resultInfo1);
                        }
                    }
                }
                foreach (AccumulatedResults.OptimizationInfo resultInfo in right.results.Values)
                {
                    SourceTable table = resultInfo.Table;
                    if (left.results[table.CollectionOrder] == null)
                        results.AddTableResult(resultInfo);
                }
            }

            internal bool ActivateFilter(int tableOrder, out bool resetFullOptimization)
            {
                AccumulatedResults.OptimizationInfo result = results[tableOrder];
                if (result == null || result.Table.CollectionOrder > tableOrder)
                {
                    resetFullOptimization = false;
                    if (optimizedResult != Triangular.Value.False)
                        return optimizedResult == Triangular.Value.Null;
                    return true;
                }
                IVistaDBIndexInformation index = SimplifyConjunction(tableOrder, out resetFullOptimization);
                if (index == null)
                {
                    if (optimizedResult != Triangular.Value.False)
                        return optimizedResult == Triangular.Value.Null;
                    return true;
                }
                IOptimizedFilter filter = result.Filter;
                if (filter != null)
                {
                    if (filter.RowCount == 0L)
                        return true;
                    result.Table.BeginOptimizedFiltering(filter, index.Name);
                }
                IRow leftScope = result.GetLeftScope(index);
                IRow rightScope = result.GetRightScope(index);
                if (leftScope != null && rightScope != null)
                {
                    result.Table.ActiveIndex = index.Name;
                    return result.Table.SetScope(leftScope, rightScope);
                }
                if (optimizedResult != Triangular.Value.False)
                    return optimizedResult == Triangular.Value.Null;
                return true;
            }

            internal string GetJoinedTable(SourceTable table)
            {
                if (rightValue != null && rightValue is ColumnSignature)
                {
                    SourceTable table1 = ((ColumnSignature)rightValue).Table;
                    if (table1.CollectionOrder == table.CollectionOrder + 1)
                        return table1.Alias;
                }
                if (leftValue != null && leftValue is ColumnSignature)
                {
                    SourceTable table1 = ((ColumnSignature)leftValue).Table;
                    if (table1.CollectionOrder == table.CollectionOrder - 1)
                        return table1.Alias;
                }
                return null;
            }
        }

        private class ColumnCompareConstraint : Constraint
        {
            internal ColumnCompareConstraint(ColumnSignature column, Signature leftConstantValue, Signature rightConstantValue, CompareOperation compareOperation, bool fts)
              : base(ConstraintType.Bitwise, column, leftConstantValue, rightConstantValue, compareOperation, fts)
            {
            }

            protected override bool NullValuesExcluded
            {
                get
                {
                    return true;
                }
            }

            protected override void OnEvalScope(IDatabase db, SourceTable table, int currentTableOrder, TableCollection sourceTables)
            {
                if (results.OptimizationIndex == null)
                    SetOptimizableResult(Triangular.Value.True);
                else
                    base.OnEvalScope(db, table, currentTableOrder, sourceTables);
            }
        }

        private class JoinColumnCompareConstraint : ColumnCompareConstraint
        {
            private string keyExpression;

            internal JoinColumnCompareConstraint(ColumnSignature column, Signature leftConstantValue, Signature rightConstantValue, CompareOperation compareOperation)
              : base(column, leftConstantValue, rightConstantValue, compareOperation, false)
            {
            }

            protected override bool NullValuesExcluded
            {
                get
                {
                    return true;
                }
            }

            protected override void OnAnalyze()
            {
                ColumnSignature leftValue = this.leftValue as ColumnSignature;
                ColumnSignature columnSignature = ReferenceEquals(this.leftValue, rightValue) ? null : rightValue as ColumnSignature;
                if (!(leftValue == null) && leftValue.Table == ColumnSignature.Table || !(columnSignature == null) && columnSignature.Table == ColumnSignature.Table)
                    return;
                base.OnAnalyze();
            }

            protected override void OnFindIndexes()
            {
                base.OnFindIndexes();
                if (Optimized)
                    return;
                keyExpression = ColumnSignature.ColumnName;
                Optimized = true;
                FullOptimized = true;
            }

            protected override void OnInitializeBuilding(IDatabase db, int currentTableOrder, TableCollection sourceTables)
            {
                if (results.OptimizationIndex == null && keyExpression != null)
                {
                    FindIndexes();
                    if (results.OptimizationIndex == null)
                    {
                        ColumnSignature.Table.CreateIndex(keyExpression, true);
                        FindIndexes();
                    }
                }
                base.OnInitializeBuilding(db, currentTableOrder, sourceTables);
            }
        }

        private class JoinColumnEqualityConstraint : JoinColumnCompareConstraint
        {
            private readonly ColumnSignature leftColumnSignature;

            internal JoinColumnEqualityConstraint(ColumnSignature rightColumn, ColumnSignature leftColumn)
              : base(rightColumn, leftColumn, leftColumn, CompareOperation.Equal)
            {
                leftColumnSignature = leftColumn;
            }

            internal ColumnSignature RightColumnSignature
            {
                get
                {
                    return ColumnSignature;
                }
            }

            internal ColumnSignature LeftColumnSignature
            {
                get
                {
                    return leftColumnSignature;
                }
            }

            protected override void OnAnalyze()
            {
                base.OnAnalyze();
                SourceTable table = RightColumnSignature.Table;
                if (!(table.OptimizedIndexColumn == null) || !(table.OptimizedKeyColumn == null))
                    return;
                int collectionOrder = table.CollectionOrder;
                IVistaDBIndexInformation optimizationIndex = results.OptimizationIndex;
                if (optimizationIndex == null)
                    return;
                string name = optimizationIndex.Name;
                IVistaDBKeyColumn[] keyStructure = optimizationIndex.KeyStructure;
                if (string.IsNullOrEmpty(results.OptimizationIndexByTableOrder(collectionOrder)) || optimizationIndex.FullTextSearch || (keyStructure == null || keyStructure[0].RowIndex != RightColumnSignature.ColumnIndex))
                    return;
                bool useCache = optimizationIndex.Unique && keyStructure.Length == 1;
                table.SetJoinOptimizationColumns(leftColumnSignature, RightColumnSignature, name, useCache);
            }

            protected override void OnFindIndexes()
            {
                base.OnFindIndexes();
            }

            protected override void OnEvalScope(IDatabase db, SourceTable table, int currentTableOrder, TableCollection sourceTables)
            {
                base.OnEvalScope(db, table, currentTableOrder, sourceTables);
            }
        }

        private class IsNullConstraint : ColumnCompareConstraint
        {
            private bool includeNulls;
            private bool originIncludeNulls;

            internal IsNullConstraint(ColumnSignature column, bool isNull)
              : base(column, null, null, CompareOperation.IsNull, false)
            {
                includeNulls = isNull;
                originIncludeNulls = isNull;
            }

            protected override void OnEvalScope(IDatabase db, SourceTable table, int currentTableOrder, TableCollection sourceTables)
            {
                base.OnEvalScope(db, table, currentTableOrder, sourceTables);
                if (includeNulls || optimizedResult != Triangular.Value.Undefined)
                    return;
                includeNulls = true;
                Invertion();
            }

            protected override void OnInitializeBuilding(IDatabase db, int currentTableOrder, TableCollection sourceTables)
            {
                includeNulls = originIncludeNulls;
                base.OnInitializeBuilding(db, currentTableOrder, sourceTables);
            }

            internal override bool ExcludeNulls
            {
                get
                {
                    return !includeNulls;
                }
            }

            protected override void OnInvertion()
            {
                if (optimizedResult != Triangular.Value.Undefined)
                {
                    base.OnInvertion();
                }
                else
                {
                    AccumulatedResults.OptimizationInfo.ScopeInfo evaluatedScope = results.EvaluatedScope;
                    if (includeNulls)
                    {
                        evaluatedScope.LeftScope.InitTop();
                        evaluatedScope.RightScope.InitBottom();
                        evaluatedScope.LeftScope.RowId = Row.MaxRowId;
                        evaluatedScope.RightScope.RowId = Row.MinRowId;
                        includeNulls = false;
                    }
                    else
                    {
                        for (int index = 0; index < evaluatedScope.RightScope.Count; ++index)
                        {
                            evaluatedScope.LeftScope[index].Value = null;
                            evaluatedScope.RightScope[index].Value = null;
                        }
                        evaluatedScope.LeftScope.RowId = Row.MinRowId + 1U;
                        evaluatedScope.RightScope.RowId = Row.MaxRowId - 1U;
                        includeNulls = true;
                    }
                }
            }

            internal override void ActivateMustBeBitmap(int tableOrder)
            {
                if (optimizedResult != Triangular.Value.Undefined)
                    return;
                results[tableOrder]?.FinalizeBitmap(this);
            }
        }

        private class ConstantsCompareConstraint : Constraint
        {
            internal ConstantsCompareConstraint(Signature leftConstant, Signature rightConstant, CompareOperation cmp)
              : base(ConstraintType.Bitwise, null, leftConstant, rightConstant, cmp, false)
            {
            }

            protected override bool NullValuesExcluded
            {
                get
                {
                    return true;
                }
            }

            protected override void OnEvalScope(IDatabase db, SourceTable table, int currentTableOrder, TableCollection sourceTables)
            {
                IColumn column1 = leftValue.Execute();
                IColumn column2 = rightValue.Execute();
                if (leftValue.IsNull || rightValue.IsNull)
                {
                    SetOptimizableResult(Triangular.Value.False);
                    alwaysNull = true;
                }
                else
                {
                    alwaysNull = false;
                    IColumn column3 = ((Row.Column)column1).Duplicate(false);
                    db.Conversion.Convert(column2, column3);
                    int num = column1.Compare(column3);
                    SetOptimizableResult(num == 0 && (compareOperation == CompareOperation.Equal || compareOperation == CompareOperation.GreaterOrEqual || compareOperation == CompareOperation.LessOrEqual) || num < 0 && (compareOperation == CompareOperation.Less || compareOperation == CompareOperation.LessOrEqual || compareOperation == CompareOperation.NotEqual) || num > 0 && (compareOperation == CompareOperation.Greater || compareOperation == CompareOperation.GreaterOrEqual || compareOperation == CompareOperation.NotEqual) ? Triangular.Value.True : Triangular.Value.False);
                }
            }

            protected override void OnInvertion()
            {
                if (IsAlwaysNull || optimizedResult == Triangular.Value.Undefined)
                    return;
                SetOptimizableResult(Triangular.Not(optimizedResult));
            }

            protected override void OnFindIndexes()
            {
                Optimized = true;
                FullOptimized = true;
            }
        }

        private class NotBundle : Constraint
        {
            internal NotBundle()
              : base(ConstraintType.Not)
            {
            }
        }

        private class AndBundle : Constraint
        {
            internal AndBundle()
              : base(ConstraintType.And)
            {
            }
        }

        private class OrBundle : Constraint
        {
            internal OrBundle()
              : base(ConstraintType.Or)
            {
            }
        }
    }
}
