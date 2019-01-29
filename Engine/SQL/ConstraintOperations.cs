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
    private ConstraintOperations.OptimizationParts constraints = new ConstraintOperations.OptimizationParts();
    private OptimizationLevel optimizationLevel;
    private IDatabase database;
    private TableCollection sourceTables;

    internal ConstraintOperations(IDatabase db, TableCollection sourceTables)
      : base(100)
    {
      this.database = db;
      this.sourceTables = sourceTables;
      this.optimizationLevel = OptimizationLevel.Full;
    }

    public override int Count
    {
      get
      {
        return this.constraints.Count;
      }
    }

    internal OptimizationLevel OptimizationLevel
    {
      get
      {
        return this.optimizationLevel;
      }
    }

    internal void ResetFullOptimizationLevel()
    {
      if (this.optimizationLevel != OptimizationLevel.Full)
        return;
      this.optimizationLevel = OptimizationLevel.Part;
    }

    private ConstraintOperations.Constraint Pop()
    {
      return (ConstraintOperations.Constraint) base.Pop();
    }

    private bool AddJoinConstraint(ColumnSignature leftColumn, ColumnSignature rightColumn, CompareOperation cmp, CompareOperation revCmp)
    {
      if (cmp == CompareOperation.Equal && leftColumn.DataType == rightColumn.DataType && leftColumn.Table.CollectionOrder != rightColumn.Table.CollectionOrder)
      {
        this.constraints.Add(leftColumn.Table.CollectionOrder <= rightColumn.Table.CollectionOrder ? (ConstraintOperations.Constraint) new ConstraintOperations.JoinColumnEqualityConstraint(rightColumn, leftColumn) : (ConstraintOperations.Constraint) new ConstraintOperations.JoinColumnEqualityConstraint(leftColumn, rightColumn));
        return true;
      }
      if (leftColumn.Table.CollectionOrder > rightColumn.Table.CollectionOrder)
        return this.AddValueConstraint(leftColumn, (Signature) rightColumn, cmp, false, true);
      return this.AddValueConstraint(rightColumn, (Signature) leftColumn, revCmp, false, true);
    }

    private bool AddNullValueConstraint(ColumnSignature column, bool isNull)
    {
      this.constraints.Add((ConstraintOperations.Constraint) new ConstraintOperations.IsNullConstraint(column, isNull));
      return true;
    }

    private bool AddValueConstraint(ColumnSignature column, Signature valueSignature, CompareOperation cmp, bool fts, bool join)
    {
      Signature leftConstantValue = (Signature) null;
      Signature rightConstantValue = (Signature) null;
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
      this.constraints.Add(join ? (ConstraintOperations.Constraint) new ConstraintOperations.JoinColumnCompareConstraint(column, leftConstantValue, rightConstantValue, cmp) : (ConstraintOperations.Constraint) new ConstraintOperations.ColumnCompareConstraint(column, leftConstantValue, rightConstantValue, cmp, fts));
      return true;
    }

    private bool AddScopeValueContraint(ColumnSignature column, Signature low, Signature high, bool fts)
    {
      this.constraints.Add((ConstraintOperations.Constraint) new ConstraintOperations.ColumnCompareConstraint(column, low, high, CompareOperation.InScope, fts));
      return true;
    }

    private bool AddConstantConstraint(Signature leftOperand, Signature rightOperand, CompareOperation cmp)
    {
      this.constraints.Add((ConstraintOperations.Constraint) new ConstraintOperations.ConstantsCompareConstraint(leftOperand, rightOperand, cmp));
      return true;
    }

    internal bool AnalyzeOptimizationLevel()
    {
      if (this.constraints.Count == 0)
        return false;
      foreach (ConstraintOperations.Constraint constraint in (List<ConstraintOperations.Constraint>) this.constraints)
      {
        constraint.Optimized = false;
        constraint.FullOptimized = false;
        if (!constraint.IsBundle)
          constraint.Analyze();
      }
      this.Clear();
      foreach (ConstraintOperations.Constraint constraint1 in (List<ConstraintOperations.Constraint>) this.constraints)
      {
        switch (constraint1.Type)
        {
          case ConstraintOperations.ConstraintType.And:
            ConstraintOperations.Constraint constraint2 = this.Pop();
            ConstraintOperations.Constraint constraint3 = this.Pop();
            constraint1.Optimized = constraint2.Optimized || constraint3.Optimized;
            constraint1.FullOptimized = constraint2.FullOptimized && constraint3.FullOptimized;
            this.Push((object) constraint1);
            continue;
          case ConstraintOperations.ConstraintType.Or:
            ConstraintOperations.Constraint constraint4 = this.Pop();
            ConstraintOperations.Constraint constraint5 = this.Pop();
            constraint1.Optimized = constraint4.Optimized && constraint5.Optimized;
            constraint1.FullOptimized = constraint4.FullOptimized && constraint5.FullOptimized;
            this.Push((object) constraint1);
            continue;
          case ConstraintOperations.ConstraintType.Not:
            ConstraintOperations.Constraint constraint6 = this.Pop();
            bool flag = constraint6.Optimized && constraint6.FullOptimized;
            constraint1.Optimized = flag;
            constraint1.FullOptimized = flag;
            this.Push((object) constraint1);
            continue;
          default:
            this.Push((object) constraint1);
            continue;
        }
      }
      ConstraintOperations.Constraint constraint7 = this.Pop();
      if (!constraint7.Optimized)
        this.optimizationLevel = OptimizationLevel.None;
      else if (!constraint7.FullOptimized)
        this.optimizationLevel = OptimizationLevel.Part;
      return this.optimizationLevel != OptimizationLevel.None;
    }

    internal bool ActivateOptimizedFilter(int tableOrder)
    {
      if (this.optimizationLevel == OptimizationLevel.None)
        return false;
      this.Clear();
      bool emptyResultSet;
      if (this.sourceTables[tableOrder].ActivateOptimizedConstraints(out emptyResultSet))
      {
        if (this.optimizationLevel == OptimizationLevel.Full)
          this.optimizationLevel = OptimizationLevel.Part;
        return emptyResultSet;
      }
      bool resetFullOptimization = false;
      foreach (ConstraintOperations.Constraint constraint1 in (List<ConstraintOperations.Constraint>) this.constraints)
      {
        switch (constraint1.Type)
        {
          case ConstraintOperations.ConstraintType.And:
            ConstraintOperations.Constraint left1 = this.Pop();
            ConstraintOperations.Constraint right1 = this.Pop();
            constraint1.Conjunction(this.database, tableOrder, left1, right1, out resetFullOptimization);
            this.Push((object) constraint1);
            break;
          case ConstraintOperations.ConstraintType.Or:
            ConstraintOperations.Constraint left2 = this.Pop();
            ConstraintOperations.Constraint right2 = this.Pop();
            constraint1.Disjunction(this.database, tableOrder, left2, right2, out resetFullOptimization);
            this.Push((object) constraint1);
            break;
          case ConstraintOperations.ConstraintType.Not:
            ConstraintOperations.Constraint constraint2 = this.Pop();
            constraint2.Invertion();
            this.Push((object) constraint2);
            break;
          default:
            constraint1.InitializeBuilding(this.database, tableOrder, this.sourceTables);
            this.Push((object) constraint1);
            break;
        }
        if (resetFullOptimization && this.optimizationLevel == OptimizationLevel.Full)
          this.optimizationLevel = OptimizationLevel.Part;
      }
      ConstraintOperations.Constraint constraint = this.Pop();
      try
      {
        return constraint.ActivateFilter(tableOrder, out resetFullOptimization);
      }
      finally
      {
        if (resetFullOptimization && this.optimizationLevel == OptimizationLevel.Full)
          this.optimizationLevel = OptimizationLevel.Part;
      }
    }

    internal bool AddLogicalBetween(ColumnSignature column, Signature low, Signature high, bool fts)
    {
      if (column.SignatureType != SignatureType.MultiplyColumn && low.SignatureType != SignatureType.Column && high.SignatureType != SignatureType.Column)
        return this.AddScopeValueContraint(column, low, high, fts);
      return false;
    }

    internal bool AddLogicalIsNull(Signature columnOperand, bool isNull)
    {
      if (columnOperand.SignatureType == SignatureType.Column)
        return this.AddNullValueConstraint((ColumnSignature) columnOperand, isNull);
      return false;
    }

    internal bool AddLogicalCompare(Signature leftOperand, Signature rightOperand, CompareOperation cmp, CompareOperation revCmp, bool fts)
    {
      if (leftOperand.SignatureType == SignatureType.Column && rightOperand.SignatureType == SignatureType.Column)
        return this.AddJoinConstraint((ColumnSignature) leftOperand, (ColumnSignature) rightOperand, cmp, revCmp);
      if (leftOperand.SignatureType == SignatureType.Column)
        return this.AddValueConstraint((ColumnSignature) leftOperand, rightOperand, cmp, fts, false);
      if (rightOperand.SignatureType == SignatureType.Column)
        return this.AddValueConstraint((ColumnSignature) rightOperand, leftOperand, revCmp, false, false);
      if ((leftOperand.SignatureType == SignatureType.Parameter || leftOperand.SignatureType == SignatureType.Constant) && (rightOperand.SignatureType == SignatureType.Parameter || rightOperand.SignatureType == SignatureType.Constant))
        return this.AddConstantConstraint(leftOperand, rightOperand, cmp);
      return false;
    }

    internal bool AddLogicalExpression(Signature operand)
    {
      return false;
    }

    internal bool AddLogicalNot()
    {
      this.constraints.Add((ConstraintOperations.Constraint) new ConstraintOperations.NotBundle());
      return true;
    }

    internal bool AddLogicalAnd()
    {
      this.constraints.Add((ConstraintOperations.Constraint) new ConstraintOperations.AndBundle());
      return true;
    }

    internal bool AddLogicalOr()
    {
      this.constraints.Add((ConstraintOperations.Constraint) new ConstraintOperations.OrBundle());
      return true;
    }

    internal bool RollBackAddedConstraints(int oldCount)
    {
      int count = this.Count - oldCount;
      if (count <= 0)
        return false;
      if (oldCount <= 0)
      {
        this.ClearConstraints();
        return true;
      }
      this.constraints.RemoveRange(oldCount, count);
      return true;
    }

    internal void ClearConstraints()
    {
      this.constraints.Clear();
      this.Clear();
    }

    internal string GetIndexName(int rowIndex, int tableOrder)
    {
      if (this.optimizationLevel == OptimizationLevel.None)
        return (string) null;
      foreach (ConstraintOperations.Constraint constraint in (List<ConstraintOperations.Constraint>) this.constraints)
      {
        if (constraint.Type == ConstraintOperations.ConstraintType.Bitwise)
        {
          string optimizedIndexName = constraint.GetOptimizedIndexName(tableOrder);
          if (optimizedIndexName != null)
            return optimizedIndexName;
        }
      }
      return (string) null;
    }

    internal string GetJoinedTable(int orOrder, SourceTable table)
    {
      if (this.optimizationLevel == OptimizationLevel.None)
        return (string) null;
      foreach (ConstraintOperations.Constraint constraint in (List<ConstraintOperations.Constraint>) this.constraints)
      {
        if (constraint.Type == ConstraintOperations.ConstraintType.Bitwise)
        {
          string joinedTable = constraint.GetJoinedTable(table);
          if (joinedTable != null)
            return joinedTable;
        }
      }
      return (string) null;
    }

    private enum ConstraintType
    {
      Bitwise,
      And,
      Or,
      Not,
    }

    private class OptimizationParts : List<ConstraintOperations.Constraint>
    {
      internal OptimizationParts()
        : base(20)
      {
      }
    }

    private class AccumulatedResults : Dictionary<int, ConstraintOperations.AccumulatedResults.OptimizationInfo>
    {
      private int keyColumnOrder = -1;
      private SourceTable table;
      private IVistaDBIndexInformation activeIndex;
      private bool descending;

      internal new ConstraintOperations.AccumulatedResults.OptimizationInfo this[int tableOrder]
      {
        get
        {
          if (!this.ContainsKey(tableOrder))
            return (ConstraintOperations.AccumulatedResults.OptimizationInfo) null;
          return base[tableOrder];
        }
      }

      internal ConstraintOperations.AccumulatedResults.OptimizationInfo.ScopeInfo EvaluatedScope
      {
        get
        {
          return this[this.table.CollectionOrder].GetScopeInfo(this.activeIndex);
        }
      }

      internal IVistaDBIndexInformation OptimizationIndex
      {
        get
        {
          return this.activeIndex;
        }
      }

      internal int KeyColumnOrder
      {
        get
        {
          return this.keyColumnOrder;
        }
      }

      internal bool Descending
      {
        get
        {
          return this.descending;
        }
      }

      internal void InitOptimization(SourceTable table, IVistaDBIndexInformation index, int keyColumnOrder, bool descending)
      {
        this.table = table;
        this.activeIndex = index;
        this.keyColumnOrder = keyColumnOrder;
        this.descending = descending;
      }

      internal void InitTableResult(IRow leftScope, IRow rightScope)
      {
        this.Add(this.table.CollectionOrder, new ConstraintOperations.AccumulatedResults.OptimizationInfo(this.table, this.activeIndex, leftScope, rightScope));
      }

      internal void AddTableResult(ConstraintOperations.AccumulatedResults.OptimizationInfo resultInfo)
      {
        this.Add(resultInfo.Table.CollectionOrder, resultInfo);
      }

      internal void CopyFrom(ConstraintOperations.AccumulatedResults accumulatedResults)
      {
        this.Clear();
        foreach (ConstraintOperations.AccumulatedResults.OptimizationInfo resultInfo in accumulatedResults.Values)
          this.AddTableResult(resultInfo);
      }

      internal string OptimizationIndexByTableOrder(int tableOrder)
      {
        if (this.table == null || this.table.CollectionOrder != tableOrder)
          return (string) null;
        if (this.activeIndex != null)
          return this.activeIndex.Name;
        return (string) null;
      }

      internal class OptimizationInfo
      {
        private ConstraintOperations.AccumulatedResults.OptimizationInfo.ScopeInfoCollection scopes = new ConstraintOperations.AccumulatedResults.OptimizationInfo.ScopeInfoCollection();
        private SourceTable table;
        private IOptimizedFilter filter;
        private IVistaDBIndexInformation filterIndex;

        internal OptimizationInfo(SourceTable table, IVistaDBIndexInformation index, IRow leftScope, IRow rightScope)
        {
          this.table = table;
          this.filterIndex = index;
          this.scopes.AddScope(index, leftScope, rightScope);
        }

        internal SourceTable Table
        {
          get
          {
            return this.table;
          }
        }

        internal IOptimizedFilter Filter
        {
          get
          {
            return this.filter;
          }
        }

        internal bool ShouldBeMerged
        {
          get
          {
            if (this.scopes.Count > 1)
              return true;
            if (this.scopes.Count == 1)
              return this.filter != null;
            return false;
          }
        }

        internal ConstraintOperations.AccumulatedResults.OptimizationInfo.ScopeInfo GetScopeInfo(IVistaDBIndexInformation index)
        {
          return this.scopes[index.Name];
        }

        internal IRow GetLeftScope(IVistaDBIndexInformation index)
        {
          return this.scopes[index.Name]?.LeftScope;
        }

        internal IRow GetRightScope(IVistaDBIndexInformation index)
        {
          return this.scopes[index.Name]?.RightScope;
        }

        private void ConvertToBitmapAndConjunction(string scopeIndexName, ConstraintOperations.Constraint parentConstraint)
        {
          ConstraintOperations.AccumulatedResults.OptimizationInfo.ScopeInfo scope = this.scopes[scopeIndexName];
          this.filterIndex = scope.Index;
          IOptimizedFilter filter = this.table.BuildFilterMap(scopeIndexName, scope.LeftScope, scope.RightScope, parentConstraint.ExcludeNulls);
          if (this.filter == null)
            this.filter = filter;
          else
            this.filter.Conjunction(filter);
          this.scopes.Remove((object) scopeIndexName);
        }

        private void ConvertToBitmapAndDisjunction(string scopeIndexName, ConstraintOperations.Constraint parentConstraint)
        {
          ConstraintOperations.AccumulatedResults.OptimizationInfo.ScopeInfo scope = this.scopes[scopeIndexName];
          this.filterIndex = scope.Index;
          IOptimizedFilter filter = this.table.BuildFilterMap(scopeIndexName, scope.LeftScope, scope.RightScope, parentConstraint.ExcludeNulls);
          if (this.filter == null)
            this.filter = filter;
          else
            this.filter.Disjunction(filter);
          this.scopes.Remove((object) scopeIndexName);
        }

        internal ConstraintOperations.AccumulatedResults.OptimizationInfo Disjunction(IDatabase db, ConstraintOperations.AccumulatedResults.OptimizationInfo rightInfo, ConstraintOperations.Constraint leftParent, ConstraintOperations.Constraint rightParent)
        {
          foreach (string key in (IEnumerable) ((Hashtable) this.scopes.Clone()).Keys)
          {
            ConstraintOperations.AccumulatedResults.OptimizationInfo.ScopeInfo scope = rightInfo.scopes[key];
            if (scope == null)
            {
              this.ConvertToBitmapAndDisjunction(key, leftParent);
            }
            else
            {
              bool emptyResult;
              if (!this.scopes[key].Disjunction(scope, out emptyResult))
              {
                rightInfo.ConvertToBitmapAndDisjunction(key, rightParent);
                this.ConvertToBitmapAndDisjunction(key, leftParent);
              }
              else if (emptyResult)
                return (ConstraintOperations.AccumulatedResults.OptimizationInfo) null;
            }
          }
          foreach (string key in (IEnumerable) ((Hashtable) rightInfo.scopes.Clone()).Keys)
          {
            if (this.scopes[key] == null)
              rightInfo.ConvertToBitmapAndDisjunction(key, rightParent);
          }
          if (this.filter != null)
          {
            this.filter.Disjunction(rightInfo.filter);
            this.filterIndex = rightInfo.filterIndex;
          }
          return this;
        }

        internal ConstraintOperations.AccumulatedResults.OptimizationInfo Conjunction(IDatabase db, ConstraintOperations.AccumulatedResults.OptimizationInfo rightInfo, ConstraintOperations.Constraint leftParent, ConstraintOperations.Constraint rightParent)
        {
          foreach (string key in (IEnumerable) ((Hashtable) this.scopes.Clone()).Keys)
          {
            ConstraintOperations.AccumulatedResults.OptimizationInfo.ScopeInfo scope = rightInfo.scopes[key];
            if (scope != null)
            {
              bool emptyResult;
              if (!this.scopes[key].Conjunction(scope, out emptyResult))
              {
                rightInfo.ConvertToBitmapAndConjunction(key, rightParent);
                this.ConvertToBitmapAndConjunction(key, leftParent);
              }
              else if (emptyResult)
                return (ConstraintOperations.AccumulatedResults.OptimizationInfo) null;
            }
          }
          foreach (string key in (IEnumerable) rightInfo.scopes.Keys)
          {
            if (this.scopes[key] == null)
              this.scopes.AddScope(rightInfo.scopes[key]);
          }
          if (this.filter != null && rightInfo.filter != null)
          {
            this.filter.Conjunction(rightInfo.filter);
            this.filterIndex = rightInfo.filterIndex;
          }
          else if (rightInfo.filter != null)
          {
            this.filter = rightInfo.filter;
            this.filterIndex = rightInfo.filterIndex;
          }
          return this;
        }

        internal void SimplifyBeforeDisjunction(ConstraintOperations.Constraint parentConstraint, bool invert)
        {
          foreach (string key in (IEnumerable) ((Hashtable) this.scopes.Clone()).Keys)
            this.ConvertToBitmapAndConjunction(key, parentConstraint);
          if (!invert)
            return;
          this.filter.Invert(true);
        }

        internal IVistaDBIndexInformation SimplifyConjunction(ConstraintOperations.Constraint parentConstraint, bool forceFinalBitmapAndInvert)
        {
          int num = 0;
          IVistaDBIndexInformation indexInformation = (IVistaDBIndexInformation) null;
          foreach (string key in (IEnumerable) ((Hashtable) this.scopes.Clone()).Keys)
          {
            if (num++ == 0)
              indexInformation = this.scopes[key].Index;
            else
              this.ConvertToBitmapAndConjunction(key, parentConstraint);
          }
          if (forceFinalBitmapAndInvert)
          {
            if (indexInformation != null)
              this.ConvertToBitmapAndConjunction(indexInformation.Name, parentConstraint);
            this.filter.Invert(true);
          }
          return indexInformation ?? this.filterIndex;
        }

        internal void FinalizeBitmap(ConstraintOperations.Constraint parentConstraint)
        {
          foreach (string key in (IEnumerable) ((Hashtable) this.scopes.Clone()).Keys)
            this.ConvertToBitmapAndConjunction(key, parentConstraint);
        }

        private class ScopeInfoCollection : InsensitiveHashtable
        {
          internal ScopeInfoCollection()
          {
          }

          internal ConstraintOperations.AccumulatedResults.OptimizationInfo.ScopeInfo this[string indexName]
          {
            get
            {
              if (!this.Contains((object) indexName))
                return (ConstraintOperations.AccumulatedResults.OptimizationInfo.ScopeInfo) null;
              return (ConstraintOperations.AccumulatedResults.OptimizationInfo.ScopeInfo) this[(object) indexName];
            }
          }

          internal void AddScope(IVistaDBIndexInformation scopeIndex, IRow leftScope, IRow rightScope)
          {
            this.Add((object) scopeIndex.Name, (object) new ConstraintOperations.AccumulatedResults.OptimizationInfo.ScopeInfo(scopeIndex, leftScope, rightScope));
          }

          internal void AddScope(ConstraintOperations.AccumulatedResults.OptimizationInfo.ScopeInfo scopeInfo)
          {
            this.Add((object) scopeInfo.Index.Name, (object) scopeInfo);
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
              return this.index;
            }
          }

          internal IRow LeftScope
          {
            get
            {
              return this.leftScope;
            }
          }

          internal IRow RightScope
          {
            get
            {
              return this.rightScope;
            }
          }

          private IRow LessOf(IRow firstScope, IRow secondScope)
          {
            if (secondScope.Compare((IVistaDBRow) firstScope) <= 0)
              return secondScope;
            return firstScope;
          }

          private IRow GreatestOf(IRow firstScope, IRow secondScope)
          {
            if (secondScope.Compare((IVistaDBRow) firstScope) >= 0)
              return secondScope;
            return firstScope;
          }

          private bool IsIntersection(ConstraintOperations.AccumulatedResults.OptimizationInfo.ScopeInfo right)
          {
            IRow leftScope1 = this.leftScope;
            IRow rightScope = this.rightScope;
            IRow leftScope2 = right.leftScope;
            return right.rightScope.CompareKey((IVistaDBRow) leftScope1) >= 0 && rightScope.CompareKey((IVistaDBRow) leftScope2) >= 0;
          }

          internal bool Conjunction(ConstraintOperations.AccumulatedResults.OptimizationInfo.ScopeInfo right, out bool emptyResult)
          {
            if (!this.IsIntersection(right))
            {
              emptyResult = false;
              return false;
            }
            IRow row1 = this.GreatestOf(this.leftScope, right.leftScope);
            IRow row2 = this.LessOf(this.rightScope, right.rightScope);
            this.leftScope = row1;
            this.rightScope = row2;
            emptyResult = this.leftScope.CompareKey((IVistaDBRow) this.rightScope) > 0;
            return true;
          }

          internal bool Disjunction(ConstraintOperations.AccumulatedResults.OptimizationInfo.ScopeInfo right, out bool emptyResult)
          {
            if (!this.IsIntersection(right))
            {
              emptyResult = false;
              return false;
            }
            IRow row1 = this.LessOf(this.leftScope, right.leftScope);
            IRow row2 = this.GreatestOf(this.rightScope, right.rightScope);
            this.leftScope = row1;
            this.rightScope = row2;
            emptyResult = this.leftScope.CompareKey((IVistaDBRow) this.rightScope) > 0;
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
      protected ConstraintOperations.AccumulatedResults results = new ConstraintOperations.AccumulatedResults();
      private ConstraintOperations.ConstraintType type;
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

      protected Constraint(ConstraintOperations.ConstraintType type)
      {
        this.compareOperation = CompareOperation.Equal;
        this.type = type;
      }

      protected Constraint(ConstraintOperations.ConstraintType type, ColumnSignature column, Signature leftValue, Signature rightValue, CompareOperation compareOpration, bool fts)
        : this(type)
      {
        this.column = column;
        this.leftValue = leftValue;
        this.rightValue = rightValue;
        this.compareOperation = compareOpration;
        this.useFtsIndex = fts;
        if (!((Signature) column != (Signature) null))
          return;
        switch (compareOpration)
        {
          case CompareOperation.NotEqual:
            this.inverted = true;
            this.originInverted = true;
            this.compareOperation = CompareOperation.Equal;
            break;
          case CompareOperation.Greater:
            if (!(rightValue == (Signature) null))
              break;
            this.inverted = true;
            this.originInverted = true;
            this.compareOperation = CompareOperation.LessOrEqual;
            this.rightValue = leftValue;
            this.leftValue = (Signature) null;
            break;
          case CompareOperation.Less:
            if (!(leftValue == (Signature) null))
              break;
            this.inverted = true;
            this.originInverted = true;
            this.compareOperation = CompareOperation.GreaterOrEqual;
            this.leftValue = rightValue;
            this.rightValue = (Signature) null;
            break;
        }
      }

      internal bool Optimized
      {
        get
        {
          return this.optimized;
        }
        set
        {
          this.optimized = value;
        }
      }

      internal bool FullOptimized
      {
        get
        {
          return this.fullOptimized;
        }
        set
        {
          this.fullOptimized = value;
        }
      }

      internal ConstraintOperations.ConstraintType Type
      {
        get
        {
          return this.type;
        }
      }

      internal bool IsBundle
      {
        get
        {
          return this.type != ConstraintOperations.ConstraintType.Bitwise;
        }
      }

      internal bool IsOrBundle
      {
        get
        {
          return this.type == ConstraintOperations.ConstraintType.Or;
        }
      }

      protected ColumnSignature ColumnSignature
      {
        get
        {
          return this.column;
        }
      }

      internal bool IsAlwaysNull
      {
        get
        {
          return this.alwaysNull;
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
          if (this.inverted)
            return true;
          foreach (ConstraintOperations.AccumulatedResults.OptimizationInfo optimizationInfo in this.results.Values)
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
        this.SetOptimizableResult(Triangular.Value.True);
        return false;
      }

      protected void AddIndex(IVistaDBIndexInformation index, int keyColumnOrder, bool descending)
      {
        if (this.results.Count > 0)
          return;
        this.results.InitOptimization(this.column.Table, index, keyColumnOrder, descending);
        this.optimized = true;
        this.fullOptimized = true;
      }

      private void EvalScope(IDatabase db, SourceTable table, int currentTableOrder, TableCollection sourceTables)
      {
        this.OnEvalScope(db, table, currentTableOrder, sourceTables);
      }

      internal void SetOptimizableResult(Triangular.Value value)
      {
        this.optimizedResult = value;
        this.fullOptimized = this.optimized = value != Triangular.Value.Undefined;
        this.results.Clear();
        this.inverted = false;
      }

      private void PropagateFrom(ConstraintOperations.Constraint constraint)
      {
        this.column = constraint.column;
        this.results.CopyFrom(constraint.results);
        this.alwaysNull = constraint.alwaysNull;
        this.inverted = constraint.inverted;
        this.optimizedResult = constraint.optimizedResult;
      }

      protected virtual void OnEvalScope(IDatabase db, SourceTable table, int currentTableOrder, TableCollection sourceTables)
      {
        if (this.optimizedResult != Triangular.Value.Undefined)
          return;
        if (table.CollectionOrder != currentTableOrder)
        {
          this.SetOptimizableResult(Triangular.Value.True);
        }
        else
        {
          ColumnSignature leftValue = this.leftValue as ColumnSignature;
          ColumnSignature signature = object.ReferenceEquals((object) this.leftValue, (object) this.rightValue) ? (ColumnSignature) null : this.rightValue as ColumnSignature;
          if ((Signature) leftValue != (Signature) null && !this.TestIfEvaluable(leftValue, sourceTables, currentTableOrder) || (Signature) signature != (Signature) null && !this.TestIfEvaluable(signature, sourceTables, currentTableOrder))
            return;
          bool descending = this.results.Descending;
          IRow indexStructure = this.column.Table.DoGetIndexStructure(this.results.OptimizationIndex.Name);
          IRow rightScope = indexStructure.CopyInstance();
          indexStructure.InitTop();
          rightScope.InitBottom();
          indexStructure.RowId = Row.MinRowId + 1U;
          rightScope.RowId = Row.MaxRowId - 1U;
          if (this.compareOperation == CompareOperation.IsNull)
          {
            for (int index = 0; index < 1; ++index)
            {
              ((IValue) indexStructure[index]).Value = (object) null;
              ((IValue) rightScope[index]).Value = (object) null;
            }
            this.results.InitTableResult(indexStructure, rightScope);
          }
          else
          {
            if (!descending && this.leftValue == (Signature) null)
              indexStructure.RowId = Row.MaxRowId;
            if (descending && this.rightValue == (Signature) null)
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
            int keyColumnOrder = this.results.KeyColumnOrder;
            IColumn column = (IColumn) null;
            if (this.leftValue != (Signature) null)
            {
              column = this.leftValue.Execute();
              if (column.IsNull && this.NullValuesExcluded)
              {
                this.SetOptimizableResult(Triangular.Value.False);
                this.alwaysNull = true;
                return;
              }
              if (this.results.OptimizationIndex.FullTextSearch)
              {
                ((IValue) row1[0]).Value = (object) (short) keyColumnOrder;
                db.Conversion.Convert((IValue) column, (IValue) row1[1]);
              }
              else
                db.Conversion.Convert((IValue) column, (IValue) row1[keyColumnOrder]);
              if (this.compareOperation == CompareOperation.Greater)
                row1.RowId = descending ? Row.MinRowId : Row.MaxRowId;
            }
            if (this.rightValue != (Signature) null)
            {
              if (!object.ReferenceEquals((object) this.leftValue, (object) this.rightValue))
              {
                column = this.rightValue.Execute();
                if (column.IsNull && this.NullValuesExcluded)
                {
                  this.SetOptimizableResult(Triangular.Value.False);
                  this.alwaysNull = true;
                  return;
                }
              }
              if (this.results.OptimizationIndex.FullTextSearch)
              {
                ((IValue) row2[0]).Value = (object) (short) keyColumnOrder;
                db.Conversion.Convert((IValue) column, (IValue) row2[1]);
              }
              else
                db.Conversion.Convert((IValue) column, (IValue) row2[keyColumnOrder]);
              if (this.compareOperation == CompareOperation.Less)
                row2.RowId = descending ? Row.MaxRowId : Row.MinRowId;
            }
            if (indexStructure.Compare((IVistaDBRow) rightScope) > 0)
              this.SetOptimizableResult(Triangular.Value.False);
            this.results.InitTableResult(indexStructure, rightScope);
          }
        }
      }

      protected virtual void OnInvertion()
      {
        if (this.IsAlwaysNull)
          this.inverted = false;
        else
          this.inverted = !this.inverted;
      }

      protected virtual void OnFindIndexes()
      {
        IVistaDBTableSchema tableSchema = this.column.Table.GetTableSchema();
        if (tableSchema == null)
          return;
        foreach (IVistaDBIndexInformation index1 in (IEnumerable<IVistaDBIndexInformation>) tableSchema.Indexes.Values)
        {
          if (!(this.useFtsIndex ^ index1.FullTextSearch))
          {
            int index2 = 0;
            for (int index3 = index1.FullTextSearch ? index1.KeyStructure.Length : 1; index2 < index3; ++index2)
            {
              IVistaDBKeyColumn vistaDbKeyColumn = index1.KeyStructure[index2];
              int rowIndex = vistaDbKeyColumn.RowIndex;
              if (rowIndex == this.column.ColumnIndex)
              {
                int keyColumnOrder = index1.FullTextSearch ? rowIndex : index2;
                this.AddIndex(index1, keyColumnOrder, vistaDbKeyColumn.Descending);
                if (index1.KeyStructure.Length == 1)
                  return;
              }
            }
          }
        }
        if (this.results.OptimizationIndex != null)
          return;
        IVistaDBIndexCollection temporaryIndexes = this.column.Table.TemporaryIndexes;
        if (temporaryIndexes == null)
          return;
        foreach (IVistaDBIndexInformation index in (IEnumerable<IVistaDBIndexInformation>) temporaryIndexes)
        {
          IVistaDBKeyColumn vistaDbKeyColumn = index.KeyStructure[0];
          if (vistaDbKeyColumn.RowIndex == this.column.ColumnIndex)
            this.AddIndex(index, 0, vistaDbKeyColumn.Descending);
        }
      }

      protected virtual void OnInitializeBuilding(IDatabase db, int currentTableOrder, TableCollection sourceTables)
      {
        this.EvalScope(db, (Signature) this.column == (Signature) null ? (SourceTable) null : this.column.Table, currentTableOrder, sourceTables);
      }

      protected virtual void OnAnalyze()
      {
        this.FindIndexes();
      }

      internal void Analyze()
      {
        if (this.leftValue != (Signature) null)
          this.leftValue.SetChanged();
        if (this.rightValue != (Signature) null)
          this.rightValue.SetChanged();
        this.OnAnalyze();
      }

      internal string GetOptimizedIndexName(int tableOrder)
      {
        if (!this.optimized || this.results == null)
          return (string) null;
        return this.results.OptimizationIndexByTableOrder(tableOrder);
      }

      internal IVistaDBIndexInformation SimplifyConjunction(int tableOrder, out bool resetFullOptimization)
      {
        bool inverted = this.inverted;
        this.inverted = false;
        if (inverted && this.results.Count > 1)
        {
          this.SetOptimizableResult(Triangular.Value.True);
          resetFullOptimization = true;
          return (IVistaDBIndexInformation) null;
        }
        resetFullOptimization = false;
        return this.results[tableOrder]?.SimplifyConjunction(this, inverted);
      }

      internal void SimplifyDisjunction(int tableOrder)
      {
        bool inverted = this.inverted;
        this.inverted = false;
        this.results[tableOrder]?.SimplifyBeforeDisjunction(this, inverted);
      }

      internal virtual void ActivateMustBeBitmap(int tableOrder)
      {
      }

      internal void FindIndexes()
      {
        this.OnFindIndexes();
      }

      internal void InitializeBuilding(IDatabase db, int currentTableOrder, TableCollection sourceTables)
      {
        this.results.Clear();
        this.optimizedResult = Triangular.Value.Undefined;
        this.inverted = this.originInverted;
        this.alwaysNull = false;
        this.OnInitializeBuilding(db, currentTableOrder, sourceTables);
      }

      internal void Invertion()
      {
        this.OnInvertion();
      }

      private bool OptimizeFullDisjunctionResult(IDatabase db, int currentTableOrder, ConstraintOperations.Constraint left, ConstraintOperations.Constraint right)
      {
        if (left.optimizedResult != Triangular.Value.Undefined && right.optimizedResult != Triangular.Value.Undefined)
        {
          this.optimizedResult = Triangular.Or(left.optimizedResult, right.optimizedResult);
          return true;
        }
        if (left.optimizedResult != Triangular.Value.Undefined)
        {
          if (left.optimizedResult == Triangular.Value.True)
          {
            this.SetOptimizableResult(Triangular.Value.True);
            return true;
          }
          this.PropagateFrom(right);
          return true;
        }
        if (right.optimizedResult == Triangular.Value.Undefined)
          return false;
        if (right.optimizedResult == Triangular.Value.True)
        {
          this.SetOptimizableResult(Triangular.Value.True);
          return true;
        }
        this.PropagateFrom(left);
        return true;
      }

      private bool OptimizeFullConjunctionResult(IDatabase db, int currentTableOrder, ConstraintOperations.Constraint left, ConstraintOperations.Constraint right)
      {
        if (left.optimizedResult != Triangular.Value.Undefined && right.optimizedResult != Triangular.Value.Undefined)
        {
          this.SetOptimizableResult(Triangular.And(left.optimizedResult, right.optimizedResult));
          return true;
        }
        if (left.optimizedResult != Triangular.Value.Undefined)
        {
          if (left.optimizedResult == Triangular.Value.False)
          {
            this.SetOptimizableResult(Triangular.Value.False);
            return true;
          }
          this.PropagateFrom(right);
          return true;
        }
        if (right.optimizedResult == Triangular.Value.Undefined)
          return false;
        if (right.optimizedResult == Triangular.Value.False)
        {
          this.SetOptimizableResult(Triangular.Value.False);
          return true;
        }
        this.PropagateFrom(left);
        return true;
      }

      internal void Disjunction(IDatabase db, int currentTableOrder, ConstraintOperations.Constraint left, ConstraintOperations.Constraint right, out bool resetFullOptimization)
      {
        this.SetOptimizableResult(Triangular.Value.Undefined);
        left.ActivateMustBeBitmap(currentTableOrder);
        right.ActivateMustBeBitmap(currentTableOrder);
        if (left.results.Count > 1 || right.results.Count > 1)
        {
          this.SetOptimizableResult(Triangular.Value.False);
          resetFullOptimization = true;
        }
        else
        {
          resetFullOptimization = false;
          if (left.ShouldBeMerged)
            left.SimplifyDisjunction(currentTableOrder);
          if (right.ShouldBeMerged)
            right.SimplifyDisjunction(currentTableOrder);
          if (this.OptimizeFullDisjunctionResult(db, currentTableOrder, left, right))
            return;
          foreach (ConstraintOperations.AccumulatedResults.OptimizationInfo optimizationInfo in left.results.Values)
          {
            SourceTable table = optimizationInfo.Table;
            if (table.CollectionOrder != currentTableOrder)
            {
              this.SetOptimizableResult(Triangular.Value.False);
              break;
            }
            ConstraintOperations.AccumulatedResults.OptimizationInfo result = right.results[table.CollectionOrder];
            if (result != null)
            {
              ConstraintOperations.AccumulatedResults.OptimizationInfo resultInfo = optimizationInfo.Disjunction(db, result, left, right);
              if (resultInfo == null)
              {
                this.SetOptimizableResult(Triangular.Value.False);
                break;
              }
              this.results.AddTableResult(resultInfo);
            }
          }
        }
      }

      internal void Conjunction(IDatabase db, int currentTableOrder, ConstraintOperations.Constraint left, ConstraintOperations.Constraint right, out bool resetFullOptimization)
      {
        this.SetOptimizableResult(Triangular.Value.Undefined);
        left.ActivateMustBeBitmap(currentTableOrder);
        right.ActivateMustBeBitmap(currentTableOrder);
        resetFullOptimization = false;
        if (left.inverted)
          left.SimplifyConjunction(currentTableOrder, out resetFullOptimization);
        if (right.inverted)
          right.SimplifyConjunction(currentTableOrder, out resetFullOptimization);
        if (this.OptimizeFullConjunctionResult(db, currentTableOrder, left, right))
          return;
        this.results.Clear();
        foreach (ConstraintOperations.AccumulatedResults.OptimizationInfo resultInfo1 in left.results.Values)
        {
          SourceTable table = resultInfo1.Table;
          if (table.CollectionOrder >= currentTableOrder)
          {
            ConstraintOperations.AccumulatedResults.OptimizationInfo result = right.results[table.CollectionOrder];
            if (result != null)
            {
              if (table.CollectionOrder == currentTableOrder)
              {
                ConstraintOperations.AccumulatedResults.OptimizationInfo resultInfo2 = resultInfo1.Conjunction(db, result, left, right);
                if (resultInfo2 == null)
                {
                  this.SetOptimizableResult(Triangular.Value.False);
                  return;
                }
                this.results.AddTableResult(resultInfo2);
              }
              else
                this.results.AddTableResult(resultInfo1);
            }
          }
        }
        foreach (ConstraintOperations.AccumulatedResults.OptimizationInfo resultInfo in right.results.Values)
        {
          SourceTable table = resultInfo.Table;
          if (left.results[table.CollectionOrder] == null)
            this.results.AddTableResult(resultInfo);
        }
      }

      internal bool ActivateFilter(int tableOrder, out bool resetFullOptimization)
      {
        ConstraintOperations.AccumulatedResults.OptimizationInfo result = this.results[tableOrder];
        if (result == null || result.Table.CollectionOrder > tableOrder)
        {
          resetFullOptimization = false;
          if (this.optimizedResult != Triangular.Value.False)
            return this.optimizedResult == Triangular.Value.Null;
          return true;
        }
        IVistaDBIndexInformation index = this.SimplifyConjunction(tableOrder, out resetFullOptimization);
        if (index == null)
        {
          if (this.optimizedResult != Triangular.Value.False)
            return this.optimizedResult == Triangular.Value.Null;
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
        if (this.optimizedResult != Triangular.Value.False)
          return this.optimizedResult == Triangular.Value.Null;
        return true;
      }

      internal string GetJoinedTable(SourceTable table)
      {
        if (this.rightValue != (Signature) null && this.rightValue is ColumnSignature)
        {
          SourceTable table1 = ((ColumnSignature) this.rightValue).Table;
          if (table1.CollectionOrder == table.CollectionOrder + 1)
            return table1.Alias;
        }
        if (this.leftValue != (Signature) null && this.leftValue is ColumnSignature)
        {
          SourceTable table1 = ((ColumnSignature) this.leftValue).Table;
          if (table1.CollectionOrder == table.CollectionOrder - 1)
            return table1.Alias;
        }
        return (string) null;
      }
    }

    private class ColumnCompareConstraint : ConstraintOperations.Constraint
    {
      internal ColumnCompareConstraint(ColumnSignature column, Signature leftConstantValue, Signature rightConstantValue, CompareOperation compareOperation, bool fts)
        : base(ConstraintOperations.ConstraintType.Bitwise, column, leftConstantValue, rightConstantValue, compareOperation, fts)
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
        if (this.results.OptimizationIndex == null)
          this.SetOptimizableResult(Triangular.Value.True);
        else
          base.OnEvalScope(db, table, currentTableOrder, sourceTables);
      }
    }

    private class JoinColumnCompareConstraint : ConstraintOperations.ColumnCompareConstraint
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
        ColumnSignature columnSignature = object.ReferenceEquals((object) this.leftValue, (object) this.rightValue) ? (ColumnSignature) null : this.rightValue as ColumnSignature;
        if (!((Signature) leftValue == (Signature) null) && leftValue.Table == this.ColumnSignature.Table || !((Signature) columnSignature == (Signature) null) && columnSignature.Table == this.ColumnSignature.Table)
          return;
        base.OnAnalyze();
      }

      protected override void OnFindIndexes()
      {
        base.OnFindIndexes();
        if (this.Optimized)
          return;
        this.keyExpression = this.ColumnSignature.ColumnName;
        this.Optimized = true;
        this.FullOptimized = true;
      }

      protected override void OnInitializeBuilding(IDatabase db, int currentTableOrder, TableCollection sourceTables)
      {
        if (this.results.OptimizationIndex == null && this.keyExpression != null)
        {
          this.FindIndexes();
          if (this.results.OptimizationIndex == null)
          {
            this.ColumnSignature.Table.CreateIndex(this.keyExpression, true);
            this.FindIndexes();
          }
        }
        base.OnInitializeBuilding(db, currentTableOrder, sourceTables);
      }
    }

    private class JoinColumnEqualityConstraint : ConstraintOperations.JoinColumnCompareConstraint
    {
      private readonly ColumnSignature leftColumnSignature;

      internal JoinColumnEqualityConstraint(ColumnSignature rightColumn, ColumnSignature leftColumn)
        : base(rightColumn, (Signature) leftColumn, (Signature) leftColumn, CompareOperation.Equal)
      {
        this.leftColumnSignature = leftColumn;
      }

      internal ColumnSignature RightColumnSignature
      {
        get
        {
          return this.ColumnSignature;
        }
      }

      internal ColumnSignature LeftColumnSignature
      {
        get
        {
          return this.leftColumnSignature;
        }
      }

      protected override void OnAnalyze()
      {
        base.OnAnalyze();
        SourceTable table = this.RightColumnSignature.Table;
        if (!((Signature) table.OptimizedIndexColumn == (Signature) null) || !((Signature) table.OptimizedKeyColumn == (Signature) null))
          return;
        int collectionOrder = table.CollectionOrder;
        IVistaDBIndexInformation optimizationIndex = this.results.OptimizationIndex;
        if (optimizationIndex == null)
          return;
        string name = optimizationIndex.Name;
        IVistaDBKeyColumn[] keyStructure = optimizationIndex.KeyStructure;
        if (string.IsNullOrEmpty(this.results.OptimizationIndexByTableOrder(collectionOrder)) || optimizationIndex.FullTextSearch || (keyStructure == null || keyStructure[0].RowIndex != this.RightColumnSignature.ColumnIndex))
          return;
        bool useCache = optimizationIndex.Unique && keyStructure.Length == 1;
        table.SetJoinOptimizationColumns(this.leftColumnSignature, this.RightColumnSignature, name, useCache);
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

    private class IsNullConstraint : ConstraintOperations.ColumnCompareConstraint
    {
      private bool includeNulls;
      private bool originIncludeNulls;

      internal IsNullConstraint(ColumnSignature column, bool isNull)
        : base(column, (Signature) null, (Signature) null, CompareOperation.IsNull, false)
      {
        this.includeNulls = isNull;
        this.originIncludeNulls = isNull;
      }

      protected override void OnEvalScope(IDatabase db, SourceTable table, int currentTableOrder, TableCollection sourceTables)
      {
        base.OnEvalScope(db, table, currentTableOrder, sourceTables);
        if (this.includeNulls || this.optimizedResult != Triangular.Value.Undefined)
          return;
        this.includeNulls = true;
        this.Invertion();
      }

      protected override void OnInitializeBuilding(IDatabase db, int currentTableOrder, TableCollection sourceTables)
      {
        this.includeNulls = this.originIncludeNulls;
        base.OnInitializeBuilding(db, currentTableOrder, sourceTables);
      }

      internal override bool ExcludeNulls
      {
        get
        {
          return !this.includeNulls;
        }
      }

      protected override void OnInvertion()
      {
        if (this.optimizedResult != Triangular.Value.Undefined)
        {
          base.OnInvertion();
        }
        else
        {
          ConstraintOperations.AccumulatedResults.OptimizationInfo.ScopeInfo evaluatedScope = this.results.EvaluatedScope;
          if (this.includeNulls)
          {
            evaluatedScope.LeftScope.InitTop();
            evaluatedScope.RightScope.InitBottom();
            evaluatedScope.LeftScope.RowId = Row.MaxRowId;
            evaluatedScope.RightScope.RowId = Row.MinRowId;
            this.includeNulls = false;
          }
          else
          {
            for (int index = 0; index < evaluatedScope.RightScope.Count; ++index)
            {
              ((IValue) evaluatedScope.LeftScope[index]).Value = (object) null;
              ((IValue) evaluatedScope.RightScope[index]).Value = (object) null;
            }
            evaluatedScope.LeftScope.RowId = Row.MinRowId + 1U;
            evaluatedScope.RightScope.RowId = Row.MaxRowId - 1U;
            this.includeNulls = true;
          }
        }
      }

      internal override void ActivateMustBeBitmap(int tableOrder)
      {
        if (this.optimizedResult != Triangular.Value.Undefined)
          return;
        this.results[tableOrder]?.FinalizeBitmap((ConstraintOperations.Constraint) this);
      }
    }

    private class ConstantsCompareConstraint : ConstraintOperations.Constraint
    {
      internal ConstantsCompareConstraint(Signature leftConstant, Signature rightConstant, CompareOperation cmp)
        : base(ConstraintOperations.ConstraintType.Bitwise, (ColumnSignature) null, leftConstant, rightConstant, cmp, false)
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
        IColumn column1 = this.leftValue.Execute();
        IColumn column2 = this.rightValue.Execute();
        if (this.leftValue.IsNull || this.rightValue.IsNull)
        {
          this.SetOptimizableResult(Triangular.Value.False);
          this.alwaysNull = true;
        }
        else
        {
          this.alwaysNull = false;
          IColumn column3 = (IColumn) ((Row.Column) column1).Duplicate(false);
          db.Conversion.Convert((IValue) column2, (IValue) column3);
          int num = column1.Compare((IVistaDBColumn) column3);
          this.SetOptimizableResult(num == 0 && (this.compareOperation == CompareOperation.Equal || this.compareOperation == CompareOperation.GreaterOrEqual || this.compareOperation == CompareOperation.LessOrEqual) || num < 0 && (this.compareOperation == CompareOperation.Less || this.compareOperation == CompareOperation.LessOrEqual || this.compareOperation == CompareOperation.NotEqual) || num > 0 && (this.compareOperation == CompareOperation.Greater || this.compareOperation == CompareOperation.GreaterOrEqual || this.compareOperation == CompareOperation.NotEqual) ? Triangular.Value.True : Triangular.Value.False);
        }
      }

      protected override void OnInvertion()
      {
        if (this.IsAlwaysNull || this.optimizedResult == Triangular.Value.Undefined)
          return;
        this.SetOptimizableResult(Triangular.Not(this.optimizedResult));
      }

      protected override void OnFindIndexes()
      {
        this.Optimized = true;
        this.FullOptimized = true;
      }
    }

    private class NotBundle : ConstraintOperations.Constraint
    {
      internal NotBundle()
        : base(ConstraintOperations.ConstraintType.Not)
      {
      }
    }

    private class AndBundle : ConstraintOperations.Constraint
    {
      internal AndBundle()
        : base(ConstraintOperations.ConstraintType.And)
      {
      }
    }

    private class OrBundle : ConstraintOperations.Constraint
    {
      internal OrBundle()
        : base(ConstraintOperations.ConstraintType.Or)
      {
      }
    }
  }
}
