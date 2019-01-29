using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core.Cryptography;
using VistaDB.Engine.Core.Indexing;
using VistaDB.Engine.Core.IO;
using VistaDB.Engine.Core.Scripting;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL;

namespace VistaDB.Engine.Core
{
  internal class ClusteredRowset : VistaDB.Engine.Core.Indexing.Index
  {
    private static int rowsetRunTimeId = 0;
    internal static string TemporaryName = "$$${0}_tmp";
    internal static ulong ModificationCounter = 0;
    private int originatorIndex = -1;
    private int createTimestampIndex = -1;
    private int updateTimestampIndex = -1;
    private IVistaDBTableSchema newSchema;
    private ClusteredRowset.LastIdentity lastIdentity;
    private bool rowLockPostponed;
    private ClusteredRowset.TriggeredRowsets triggeredRowsets;
    private ClusteredRowset.EventDelegateList eventDelegates;
    private bool postponedClosing;
    private AlterList alterList;
    private TransactionLogRowset transactionLog;
    private InsensitiveHashtable selfRelations;
    private Database.ClrTriggerCollection triggersAfterInsert;
    private Database.ClrTriggerCollection triggersAfterUpdate;
    private Database.ClrTriggerCollection triggersAfterDelete;
    private RowIdFilter optimizedRowFilter;
    private Table.TableType type;
    private bool suppressAutoValues;
    private bool allowSyncEdit;

    internal static ClusteredRowset CreateInstance(string tableName, Database parentDatabase, ClusteredRowset clonedOrigin, Table.TableType type)
    {
      ++ClusteredRowset.rowsetRunTimeId;
      string alias = "TCR" + ClusteredRowset.rowsetRunTimeId.ToString();
      ClusteredRowset clusteredRowset = new ClusteredRowset(tableName, alias, parentDatabase, parentDatabase.ParentConnection, parentDatabase.Parser, parentDatabase.Encryption, clonedOrigin, type);
      clusteredRowset.DoAfterConstruction(parentDatabase.PageSize, parentDatabase.Culture);
      return clusteredRowset;
    }

    protected ClusteredRowset(string tableName, string alias, Database parentDatabase, DirectConnection connection, Parser parser, Encryption encryption, ClusteredRowset clonedOrigin, Table.TableType type)
      : base(tableName, alias, parser, connection, parentDatabase, encryption, (VistaDB.Engine.Core.Indexing.Index) clonedOrigin)
    {
      this.triggeredRowsets = new ClusteredRowset.TriggeredRowsets(this);
      this.eventDelegates = new ClusteredRowset.EventDelegateList();
      this.type = type;
    }

    internal IVistaDBTableSchema NewSchema
    {
      get
      {
        return this.newSchema;
      }
    }

    internal ClusteredRowset.ClusteredRowsetHeader Header
    {
      get
      {
        return (ClusteredRowset.ClusteredRowsetHeader) base.Header;
      }
    }

    internal Row LastSessionIdentity
    {
      get
      {
        return this.lastIdentity.Value;
      }
    }

    internal Row LastTableIdentity
    {
      get
      {
        this.LockStorage();
        try
        {
          return this.lastIdentity.GetTableIdentity(this.DefaultRow);
        }
        finally
        {
          this.UnlockStorage(true);
        }
      }
    }

    internal bool IsSystemTable
    {
      get
      {
        return this.type != Table.TableType.Default;
      }
    }

    internal bool IsAlterTemporaryTable
    {
      get
      {
        if (this.newSchema != null)
          return ((Table.TableSchema) this.newSchema).TemporarySchema;
        return false;
      }
    }

    internal bool AllowSyncEdit
    {
      get
      {
        return this.allowSyncEdit;
      }
      set
      {
        this.allowSyncEdit = value;
      }
    }

    internal bool PostponedUserUnlock
    {
      get
      {
        return this.rowLockPostponed;
      }
      set
      {
        this.rowLockPostponed = value;
      }
    }

    internal bool PostponedClosing
    {
      get
      {
        return this.postponedClosing;
      }
    }

    internal AlterList AlterList
    {
      get
      {
        return this.alterList;
      }
      set
      {
        this.alterList = value;
      }
    }

    internal uint MaxRowId
    {
      get
      {
        return this.Header.CurrentAutoId;
      }
    }

    internal bool SuppressAutoValues
    {
      get
      {
        return this.suppressAutoValues;
      }
      set
      {
        this.suppressAutoValues = value;
      }
    }

    private int[] ComparingMask
    {
      get
      {
        if (this.DefaultRow != null)
          return this.DefaultRow.ComparingMask;
        return (int[]) null;
      }
    }

    private void EnterModification()
    {
      this.LockStorage();
    }

    private void ExitModification(bool instantly)
    {
      this.UnlockStorage(instantly);
    }

    private void InitUpdatedRow(Row oldRow, Row newRow)
    {
      newRow.CopyMetaData(oldRow);
      newRow.RowVersion = this.TransactionId;
      if (!this.SuppressAutoValues)
      {
        if (oldRow.HasTimestamp)
          newRow.SetTimestamp(this.Header.NextTimestampId);
        if (this.ActiveSyncService)
        {
          newRow.SetTimestamp(this.Header.NextTimestampId, this.updateTimestampIndex);
          newRow.SetOriginator(this.Originator, this.originatorIndex, !this.AllowSyncEdit);
        }
      }
      this.AddTransactionLogEvent(this.TransactionId);
    }

    private void AddTransactionLogEvent(uint transactionId)
    {
      if (transactionId == 0U)
        return;
      if (this.transactionLog == null)
        this.transactionLog = this.DoCreateTpLog(false);
      this.transactionLog.RegisterTransaction(false, transactionId);
    }

    private void AddTombstoneRow(Row oldRow)
    {
      Table triggeredTable = this.OpenTombstoneTable();
      if (triggeredTable == null)
        return;
      ClusteredRowset rowset = triggeredTable.Rowset;
      rowset.SatelliteRow.Copy(oldRow);
      rowset.SatelliteRow[rowset.updateTimestampIndex].Value = (object) (long) this.Header.NextTimestampId;
      rowset.SatelliteRow[rowset.originatorIndex].Value = (object) this.Originator;
      rowset.CreateRow(false, false);
      if (this.WrapperDatabase == null)
        return;
      this.WrapperDatabase.AddTriggeredDependence(triggeredTable, !triggeredTable.IsClone);
    }

    private void TestTransactionUpdate(Row oldRow)
    {
      if (this.transactionLog == null)
        return;
      uint transactionId = oldRow.TransactionId;
      if ((int) transactionId != (int) this.TransactionId && oldRow.OutdatedStatus && this.DoGettingAnotherTransactionStatus(transactionId) == TpStatus.Active)
        throw new VistaDBException(455);
    }

    internal void UpdateSchemaVersion()
    {
      ++this.Header.SchemaVersion;
      this.FlushStorageVersion();
    }

    internal void ActivateIdentity(Row.Column column, string step)
    {
      EvalStack evaluation = this.Parser.Compile(Identity.SystemName + "(" + column.Name + "," + step + ")", (DataStorage) this, true);
      this.DeactivateIdentity(column);
      this.DeactivateDefaultValue(column);
      this.lastIdentity.AddColumn(column);
      this.AttachFilter((Filter) new Identity(evaluation));
      this.ActivateReadOnly(column);
      this.WrapperDatabase.ActivateLastIdentity(this.Name, column);
    }

    internal void DeactivateIdentity(Row.Column column)
    {
      this.DetachIdentityFilter(column);
      if (!column.ReadOnly)
        this.DeactivateReadOnly(column);
      this.lastIdentity.DropColumn(column);
    }

    internal void CreateIdentity(string columnName, string seedExpression, string stepExpression)
    {
      this.WrapperDatabase.LockStorage();
      bool commit = true;
      try
      {
        this.LockStorage();
        if (this.IsReadOnly)
          throw new VistaDBException(337, this.Name);
        try
        {
          Row.Column column1 = this.LookForColumn(columnName);
          if (column1 == (Row.Column) null)
            throw new VistaDBException(181, columnName);
          switch (column1.Type)
          {
            case VistaDBType.SmallInt:
            case VistaDBType.Int:
            case VistaDBType.BigInt:
              Row.Column column2 = seedExpression == null ? this.DefaultRow[column1.RowIndex].Duplicate(false) : this.CompileRow(seedExpression, true)[0];
              Row.Column column3 = this.CompileRow(stepExpression, true)[0];
              if (column2.IsNull)
                throw new VistaDBException(188, seedExpression);
              if (column3.IsNull)
                throw new VistaDBException(189, stepExpression);
              try
              {
                switch (column1.Type)
                {
                  case VistaDBType.SmallInt:
                    int num = (int) short.Parse(stepExpression);
                    break;
                  case VistaDBType.Int:
                    int.Parse(stepExpression);
                    break;
                  case VistaDBType.BigInt:
                    long.Parse(stepExpression);
                    break;
                }
              }
              catch (Exception ex)
              {
                throw new VistaDBException(ex, 190, stepExpression);
              }
              this.Conversion.Convert((IValue) column2, (IValue) this.DefaultRow[column1.RowIndex]);
              ++this.DefaultRow.RowVersion;
              this.ActivateIdentity(column1, column3.Value.ToString());
              this.WrapperDatabase.RegisterIdentity(this, column1.Name, stepExpression);
              commit = true;
              break;
            default:
              throw new VistaDBException(191, column1.Name);
          }
        }
        catch (Exception ex)
        {
          this.WrapperDatabase.ReactivateObjects(this);
          throw;
        }
        finally
        {
          this.UnlockStorage(false);
          this.FinalizeChanges(!commit, commit);
        }
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 186, columnName);
      }
      finally
      {
        this.WrapperDatabase.UnlockStorage(false);
        this.WrapperDatabase.FinalizeChanges(!commit, commit);
      }
    }

    internal void DropIdentity(string columnName)
    {
      this.WrapperDatabase.LockStorage();
      bool commit = false;
      try
      {
        if (this.IsReadOnly)
          throw new VistaDBException(337, this.Name);
        this.LockStorage();
        try
        {
          Row.Column column = this.LookForColumn(columnName);
          if (column == (Row.Column) null)
            throw new VistaDBException(181, columnName);
          this.DefaultRow[column.RowIndex].Value = (object) null;
          ++this.DefaultRow.RowVersion;
          this.WrapperDatabase.UnregisterIdentity(this, column.Name, true);
          this.DeactivateIdentity(column);
          commit = true;
        }
        finally
        {
          this.UnlockStorage(false);
          this.FinalizeChanges(!commit, commit);
        }
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 187, columnName);
      }
      finally
      {
        this.WrapperDatabase.UnlockStorage(false);
        this.WrapperDatabase.FinalizeChanges(!commit, commit);
      }
    }

    internal void ActivateDefaultValue(Row.Column column, string scriptExpression, bool useInUpdate)
    {
      scriptExpression = column.Name + ":" + scriptExpression;
      try
      {
        EvalStack evaluation = this.Parser.Compile(scriptExpression, (DataStorage) this, true);
        this.DeactivateDefaultValue(column);
        if (useInUpdate)
          this.AttachFilter((Filter) new DefaultValue(evaluation, Filter.FilterType.DefaultValueUpdateGenerator, false));
        this.AttachFilter((Filter) new DefaultValue(evaluation, Filter.FilterType.DefaultValueInsertGenerator, true));
      }
      catch (VistaDBException ex)
      {
      }
      catch (Exception ex)
      {
      }
    }

    internal void DeactivateDefaultValue(Row.Column column)
    {
      this.DetachDefaultValueFilter(column);
    }

    internal void CreateDefaultValue(string columnName, string scriptExpression, bool useInUpdate, string description)
    {
      this.WrapperDatabase.LockStorage();
      bool commit = false;
      try
      {
        this.LockStorage();
        if (this.IsReadOnly)
          throw new VistaDBException(337, this.Name);
        try
        {
          Row.Column column = this.LookForColumn(columnName);
          if (column == (Row.Column) null)
            throw new VistaDBException(181, columnName);
          switch (column.Type)
          {
            case VistaDBType.Image:
            case VistaDBType.VarBinary:
            case VistaDBType.Timestamp:
              throw new VistaDBException(198, column.Name);
            default:
              if (this.WrapperDatabase.IsIdentityRegistered(this, column.Name))
                throw new VistaDBException(184, column.Name);
              if (column.InternalType == VistaDBType.NChar)
              {
                string strB = scriptExpression.Trim(' ', char.MinValue, '(', ')');
                if (strB.Length > 1 && strB[0] != '\'' && (strB[strB.Length - 1] != '\'' && string.Compare("NULL", strB, StringComparison.OrdinalIgnoreCase) != 0))
                  scriptExpression = "'" + strB.Replace("'", "''") + "'";
              }
              EvalStack evalStack = useInUpdate ? this.Parser.Compile(scriptExpression, (DataStorage) this, false) : (EvalStack) null;
              if (evalStack != null && evalStack.IsConstantResult)
              {
                this.Conversion.Convert((IValue) this.CompileRow(scriptExpression, true)[0], (IValue) this.DefaultRow[column.RowIndex]);
                ++this.DefaultRow.RowVersion;
                if (this.WrapperDatabase.IsDefaultValueRegistered(this, column.Name))
                {
                  this.WrapperDatabase.UnregisterDefaultValue(this, column.Name, false);
                  this.DeactivateDefaultValue(column);
                }
              }
              else
              {
                this.DefaultRow[column.RowIndex].Value = (object) null;
                ++this.DefaultRow.RowVersion;
                this.ActivateDefaultValue(column, scriptExpression, useInUpdate);
                this.WrapperDatabase.RegisterDefaultValue(this, column.Name, scriptExpression, useInUpdate, description);
              }
              commit = true;
              break;
          }
        }
        catch (Exception ex)
        {
          this.WrapperDatabase.ReactivateObjects(this);
          throw ex;
        }
        finally
        {
          this.UnlockStorage(false);
          this.FinalizeChanges(!commit, commit);
        }
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 193, columnName);
      }
      finally
      {
        this.WrapperDatabase.UnlockStorage(false);
        this.WrapperDatabase.FinalizeChanges(!commit, commit);
      }
    }

    internal void DropDefaultValue(string columnName)
    {
      this.WrapperDatabase.LockStorage();
      bool commit = false;
      try
      {
        if (this.IsReadOnly)
          throw new VistaDBException(337, this.Name);
        this.LockStorage();
        try
        {
          Row.Column column = this.LookForColumn(columnName);
          if (column == (Row.Column) null)
            throw new VistaDBException(181, columnName);
          this.DefaultRow[column.RowIndex].Value = (object) null;
          ++this.DefaultRow.RowVersion;
          this.WrapperDatabase.UnregisterDefaultValue(this, column.Name, true);
          this.DeactivateDefaultValue(column);
          commit = true;
        }
        finally
        {
          this.UnlockStorage(false);
          this.FinalizeChanges(!commit, commit);
        }
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 194, columnName);
      }
      finally
      {
        this.WrapperDatabase.UnlockStorage(false);
        this.WrapperDatabase.FinalizeChanges(!commit, commit);
      }
    }

    internal void ActivateConstraint(string name, string expression, int options, DataStorage activeOrder, bool foreignKeyConstraint)
    {
      if (options == 0)
        return;
      this.DeactivateConstraint(name);
      CheckStatement checkConstraint = (CheckStatement) null;
      if (!foreignKeyConstraint)
      {
        checkConstraint = this.WrapperDatabase.SQLContext.CreateCheckConstraint(expression, this.Name, this.DefaultRow);
        int num = (int) checkConstraint.PrepareQuery();
      }
      if (Constraint.InsertionActivity(options))
        this.AttachFilter(foreignKeyConstraint ? (Filter) new FKConstraint(name, this.Parser.Compile(expression, activeOrder, true), Filter.FilterType.ConstraintAppend) : (Filter) new SQLConstraint(name, Filter.FilterType.ConstraintAppend, checkConstraint, expression));
      if (Constraint.UpdateActivity(options))
        this.AttachFilter(foreignKeyConstraint ? (Filter) new FKConstraint(name, this.Parser.Compile(expression, activeOrder, true), Filter.FilterType.ConstraintUpdate) : (Filter) new SQLConstraint(name, Filter.FilterType.ConstraintUpdate, checkConstraint, expression));
      if (!Constraint.DeleteActivity(options))
        return;
      this.AttachFilter(foreignKeyConstraint ? (Filter) new FKConstraint(name, this.Parser.Compile(expression, activeOrder, true), Filter.FilterType.ConstraintDelete) : (Filter) new SQLConstraint(name, Filter.FilterType.ConstraintDelete, checkConstraint, expression));
    }

    internal void DeactivateConstraint(string name)
    {
      this.DetachConstraintFilter(name);
    }

    internal void CreateConstraint(string name, string scriptExpression, string description, bool insertion, bool update, bool delete)
    {
      this.WrapperDatabase.LockStorage();
      bool commit = false;
      Exception exception = (Exception) null;
      try
      {
        this.LockStorage();
        if (this.IsReadOnly)
          throw new VistaDBException(337, this.Name);
        try
        {
          int options = Constraint.MakeStatus(insertion, update, delete);
          this.ActivateConstraint(name, scriptExpression, options, (DataStorage) this, false);
          this.WrapperDatabase.RegisterConstraint(this, name, scriptExpression, options, description);
          commit = true;
        }
        catch (Exception ex)
        {
          exception = ex;
          this.WrapperDatabase.ReactivateObjects(this);
          throw ex;
        }
        finally
        {
          this.UnlockStorage(false);
          this.FinalizeChanges(!commit, commit);
        }
      }
      catch (Exception ex)
      {
        Exception e = ex;
        if (exception != null)
          e = exception;
        throw new VistaDBException(e, 196, name);
      }
      finally
      {
        this.WrapperDatabase.UnlockStorage(false);
        this.WrapperDatabase.FinalizeChanges(!commit, commit);
      }
    }

    internal void DropConstraint(string name)
    {
      this.WrapperDatabase.LockStorage();
      bool commit = false;
      try
      {
        if (this.IsReadOnly)
          throw new VistaDBException(337, this.Name);
        this.LockStorage();
        try
        {
          this.WrapperDatabase.UnregisterConstraint(this, name, true);
          this.DeactivateConstraint(name);
          commit = true;
        }
        finally
        {
          this.UnlockStorage(false);
          this.FinalizeChanges(!commit, commit);
        }
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 197, name);
      }
      finally
      {
        this.WrapperDatabase.UnlockStorage(false);
        this.WrapperDatabase.FinalizeChanges(!commit, commit);
      }
    }

    internal void UpdateIndexInformation(RowsetIndex index, IVistaDBIndexInformation newIndex, bool commit)
    {
      this.WrapperDatabase.LockStorage();
      bool flag = false;
      try
      {
        if (this.IsReadOnly)
          throw new VistaDBException(337, this.Name);
        this.LockStorage();
        try
        {
          this.WrapperDatabase.UpdateRegisteredIndex(index, newIndex, commit);
          flag = true;
        }
        finally
        {
          this.UnlockStorage(false);
          this.FinalizeChanges(!flag, commit);
        }
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 131, index.Alias);
      }
      finally
      {
        this.WrapperDatabase.UnlockStorage(false);
        this.WrapperDatabase.FinalizeChanges(!flag, commit);
      }
    }

    internal void DropIndex(RowsetIndex index, bool commit)
    {
      this.WrapperDatabase.LockStorage();
      bool flag = false;
      try
      {
        if (this.IsReadOnly)
          throw new VistaDBException(337, this.Name);
        if (index.IsForeignKey)
          throw new VistaDBException(132, index.Alias);
        this.LockStorage();
        try
        {
          this.WrapperDatabase.UnregisterIndex(index);
          flag = true;
        }
        finally
        {
          this.UnlockStorage(false);
          this.FinalizeChanges(!flag, commit);
        }
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 131, index.Alias);
      }
      finally
      {
        this.WrapperDatabase.UnlockStorage(false);
        this.WrapperDatabase.FinalizeChanges(!flag, commit);
      }
    }

    internal void RenameIndex(RowsetIndex index, string oldName, string newName, bool commit)
    {
      this.WrapperDatabase.LockStorage();
      bool flag = false;
      try
      {
        if (this.IsReadOnly)
          throw new VistaDBException(337, this.Name);
        if (index.IsForeignKey)
          throw new VistaDBException(159, index.Alias);
        this.LockStorage();
        try
        {
          this.WrapperDatabase.UnregisterIndex(index);
          index.Alias = newName;
          this.WrapperDatabase.RegisterIndex(index);
          flag = true;
        }
        finally
        {
          this.UnlockStorage(false);
          this.FinalizeChanges(!flag, commit);
        }
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 131, index.Alias);
      }
      finally
      {
        this.WrapperDatabase.UnlockStorage(false);
        this.WrapperDatabase.FinalizeChanges(!flag, commit);
      }
    }

    internal void ActivateReadOnly(Row.Column column)
    {
      EvalStack evaluation = this.Parser.Compile(Readonly.SystemName + "(" + column.Name + ")", (DataStorage) this, true);
      this.DeactivateReadOnly(column);
      this.AttachFilter((Filter) new Readonly(column.Name, evaluation));
    }

    internal void DeactivateReadOnly(Row.Column column)
    {
      this.DetachReadonlyFilter(column.Name);
    }

    internal void CreateForeignKey(Table parentFkTable, string constraintName, string foreignKey, string primaryTable, VistaDBReferentialIntegrity updateIntegrity, VistaDBReferentialIntegrity deleteIntegrity, string description)
    {
      Table table = (Table) null;
      this.WrapperDatabase.LockStorage();
      bool commit = false;
      try
      {
        if (this.IsReadOnly)
          throw new VistaDBException(337, this.Name);
        this.LockStorage();
        try
        {
          if (!this.WrapperDatabase.ContainsPrimaryKey(primaryTable))
            throw new VistaDBException(202, primaryTable);
          EvalStack fkEvaluator = this.Parser.Compile(foreignKey, (DataStorage) this, false);
          ClusteredRowset rowset = parentFkTable.Rowset;
          table = (Table) this.WrapperDatabase.OpenClone(primaryTable, rowset.IsReadOnly);
          foreach (VistaDB.Engine.Core.Indexing.Index index in table.Values)
          {
            if (index.IsPrimary)
            {
              if (!index.DoCheckIfRelated(fkEvaluator))
                throw new VistaDBException(203, foreignKey);
              if (Database.DatabaseObject.EqualNames(table.Name, this.Name))
              {
                if (index.DoCheckIfSame(fkEvaluator))
                  throw new VistaDBException(206, foreignKey);
                break;
              }
              break;
            }
          }
          parentFkTable.CreateIndex(constraintName, foreignKey, (string) null, false, false, false, false, true, false, false);
          this.WrapperDatabase.RegisterForeignKey(constraintName, this, table.Rowset, foreignKey, updateIntegrity, deleteIntegrity, description);
          this.ActivateForeignKey(parentFkTable, constraintName, table.Rowset.Name);
          commit = true;
        }
        finally
        {
          this.UnlockStorage(false);
          this.FinalizeChanges(!commit, commit);
        }
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 200, constraintName);
      }
      finally
      {
        this.WrapperDatabase.UnlockStorage(false);
        this.WrapperDatabase.FinalizeChanges(!commit, commit);
        this.WrapperDatabase.ReleaseClone((ITable) table);
      }
    }

    internal void DropForeignKey(Table parentFkTable, RowsetIndex foreignIndex, string constraintName, bool commit)
    {
      this.WrapperDatabase.LockStorage();
      bool flag = false;
      try
      {
        if (this.IsReadOnly)
          throw new VistaDBException(337, this.Name);
        this.LockStorage();
        try
        {
          this.WrapperDatabase.UnregisterForeignKey(constraintName, this);
          if (foreignIndex.IsForeignKey)
            this.WrapperDatabase.UnregisterIndex(foreignIndex);
          this.DeactivateForeignKey(parentFkTable, constraintName);
          flag = true;
        }
        finally
        {
          this.UnlockStorage(false);
          this.FinalizeChanges(!flag, commit);
        }
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 201, constraintName);
      }
      finally
      {
        this.WrapperDatabase.UnlockStorage(false);
        this.WrapperDatabase.FinalizeChanges(!flag, commit);
      }
    }

    internal void ActivateForeignKey(Table fkTable, string fkIndexName, string pkTableName)
    {
      VistaDB.Engine.Core.Indexing.Index indexOrder = fkTable.GetIndexOrder(fkIndexName, true);
      string keyExpression = ((RowsetIndex) indexOrder).KeyExpression;
      this.ActivateConstraint(fkTable.Rowset.Name + "." + fkIndexName, ForeignKeyConstraint.ReferencedKey + "( '" + pkTableName + "' )", Constraint.MakeStatus(true, true, false), (DataStorage) indexOrder, true);
    }

    internal void DeactivateForeignKey(Table fkTable, string fkIndexName)
    {
      this.DeactivateConstraint(fkTable.Rowset.Name + "." + fkIndexName);
    }

    internal void ActivatePrimaryKeyReference(Table pkTable, string fkTableName, string fkIndexName, VistaDBReferentialIntegrity updateIntegrity, VistaDBReferentialIntegrity deleteIntegrity)
    {
      VistaDB.Engine.Core.Indexing.Index indexOrder = pkTable.GetIndexOrder(pkTable.PKIndex, true);
      this.ActivateConstraint(pkTable.Rowset.Name + ".update." + fkIndexName, NonreferencedPrimaryKey.NonReferencedKey + "('" + fkTableName + "','" + fkIndexName + "'," + ((int) updateIntegrity).ToString() + ")", Constraint.MakeStatus(false, true, false), (DataStorage) indexOrder, true);
      this.ActivateConstraint(pkTable.Rowset.Name + ".delete." + fkIndexName, NonreferencedPrimaryKey.NonReferencedKey + "('" + fkTableName + "','" + fkIndexName + "'," + ((int) deleteIntegrity).ToString() + ")", Constraint.MakeStatus(false, false, true), (DataStorage) indexOrder, true);
    }

    internal void DeactivatePrimaryKeyReference(Table pkTable, string fkIndexName)
    {
      this.DeactivateConstraint(pkTable.Rowset.Name + ".update." + fkIndexName);
      this.DeactivateConstraint(pkTable.Rowset.Name + ".delete." + fkIndexName);
    }

    internal void AddTriggeredDependence(Table triggeredTable, bool closeByFinalization)
    {
      this.triggeredRowsets.AddFiredTable(triggeredTable, closeByFinalization);
    }

    internal void SetDelegate(IVistaDBDDAEventDelegate eventDelegate)
    {
      this.eventDelegates.SetDelegate(eventDelegate);
    }

    internal void ResetDelegate(DDAEventDelegateType type)
    {
      this.eventDelegates.ResetDelegate(type);
    }

    internal void CloseTriggeredTables()
    {
      this.triggeredRowsets.CloseTriggered();
    }

    internal void SaveSelfRelationship(IVistaDBRelationshipInformation relationship)
    {
      if (this.selfRelations == null)
        this.selfRelations = new InsensitiveHashtable();
      if (this.selfRelations.ContainsKey((object) relationship.Name))
        return;
      this.selfRelations.Add((object) relationship.Name, (object) relationship);
    }

    internal void FreezeSelfRelationships(Table table)
    {
      if (this.selfRelations == null)
        return;
      foreach (IVistaDBRelationshipInformation selfRelation in (Hashtable) this.selfRelations)
      {
        this.DeactivatePrimaryKeyReference(table, selfRelation.Name);
        this.DeactivateForeignKey(table, selfRelation.Name);
      }
    }

    internal void DefreezeSelfRelationships(Table table)
    {
      if (this.selfRelations == null)
        return;
      foreach (IVistaDBRelationshipInformation selfRelation in (Hashtable) this.selfRelations)
      {
        this.ActivatePrimaryKeyReference(table, this.Name, selfRelation.Name, selfRelation.UpdateIntegrity, selfRelation.DeleteIntegrity);
        this.ActivateForeignKey(table, selfRelation.Name, this.Name);
      }
    }

    internal void ActivateTriggers(IVistaDBTableSchema schema)
    {
      foreach (Database.ClrTriggerCollection.ClrTriggerInformation trigger in (IEnumerable<IVistaDBClrTriggerInformation>) schema.Triggers)
      {
        if (trigger.Active)
        {
          if ((((IVistaDBClrTriggerInformation) trigger).TriggerAction & TriggerAction.AfterInsert) == TriggerAction.AfterInsert)
          {
            if (this.triggersAfterInsert == null)
              this.triggersAfterInsert = new Database.ClrTriggerCollection();
            this.triggersAfterInsert.AddTrigger(trigger);
          }
          if ((((IVistaDBClrTriggerInformation) trigger).TriggerAction & TriggerAction.AfterUpdate) == TriggerAction.AfterUpdate)
          {
            if (this.triggersAfterUpdate == null)
              this.triggersAfterUpdate = new Database.ClrTriggerCollection();
            this.triggersAfterUpdate.AddTrigger(trigger);
          }
          if ((((IVistaDBClrTriggerInformation) trigger).TriggerAction & TriggerAction.AfterDelete) == TriggerAction.AfterDelete)
          {
            if (this.triggersAfterDelete == null)
              this.triggersAfterDelete = new Database.ClrTriggerCollection();
            this.triggersAfterDelete.AddTrigger(trigger);
          }
        }
      }
    }

    internal void DeactivateTriggers()
    {
      if (this.triggersAfterInsert != null)
        this.triggersAfterInsert.Clear();
      this.triggersAfterInsert = (Database.ClrTriggerCollection) null;
      if (this.triggersAfterUpdate != null)
        this.triggersAfterUpdate.Clear();
      this.triggersAfterUpdate = (Database.ClrTriggerCollection) null;
      if (this.triggersAfterDelete != null)
        this.triggersAfterDelete.Clear();
      this.triggersAfterDelete = (Database.ClrTriggerCollection) null;
    }

    protected virtual void DoCopyNonEdited(Row sourceRow, Row destinRow)
    {
      if (this.SuppressAutoValues)
        return;
      bool flag = sourceRow == this.DefaultRow;
      int index = 0;
      for (int count = destinRow.Count; index < count; ++index)
      {
        Row.Column column = destinRow[index];
        if (column.ExtendedType)
          ((ExtendedColumn) column).ResetMetaValue();
        if (!column.Edited)
        {
          Row.Column srcColumn = sourceRow[index];
          if (flag)
          {
            column.Value = srcColumn.Value;
            column.Edited = !srcColumn.IsNull;
          }
          else
            column.CreateFullCopy(srcColumn);
        }
      }
    }

    protected virtual void DoInitCreatedRow(Row newRow)
    {
      newRow.RowId = this.Header.AutoId;
      newRow.RowVersion = this.TransactionId;
      newRow.RefPosition = Row.EmptyReference;
      if (!this.SuppressAutoValues)
      {
        if (newRow.HasTimestamp)
          newRow.SetTimestamp(this.Header.NextTimestampId);
        if (this.ActiveSyncService)
        {
          ulong nextTimestampId = this.Header.NextTimestampId;
          newRow.SetTimestamp(nextTimestampId, this.createTimestampIndex);
          newRow.SetTimestamp(nextTimestampId, this.updateTimestampIndex);
          newRow.SetOriginator(this.Originator, this.originatorIndex, !this.AllowSyncEdit);
        }
      }
      this.AddTransactionLogEvent(this.TransactionId);
    }

    protected virtual bool DoAssignIdentity(Row newRow)
    {
      this.lastIdentity.AssignValue(newRow);
      return true;
    }

    protected virtual bool DoCheckNulls(Row row)
    {
      foreach (Row.Column column in (List<Row.Column>) row)
      {
        if (!column.AllowNull && column.IsNull)
          throw new VistaDBException(174, column.Name);
      }
      return true;
    }

    protected virtual bool DoAllocateExtensions(Row newRow, bool fresh)
    {
      newRow.WriteExtensions((DataStorage) this, fresh, true);
      return true;
    }

    protected virtual bool DoDeallocateExtensions(Row oldRow)
    {
      if (!this.IsTransaction)
        return oldRow.FreeExtensionSpace((DataStorage) this);
      return true;
    }

    protected virtual bool DoReallocateExtensions(Row oldRow, Row newRow)
    {
      if (this.IsTransaction)
        return this.DoAllocateExtensions(newRow, false);
      RowExtension extensions = newRow.Extensions;
      if (extensions == null)
        return true;
      foreach (ExtendedColumn extendedColumn in (List<IColumn>) extensions)
      {
        if (extendedColumn.NeedFlush)
          ((ExtendedColumn) oldRow[extendedColumn.RowIndex]).FreeSpace((DataStorage) this);
      }
      newRow.WriteExtensions((DataStorage) this, false, true);
      return true;
    }

    protected virtual bool DoMirrowModifications(Row oldRow, Row newRow, TriggerAction triggerAction)
    {
      if (this.GetAvailableTriggers(triggerAction) == null || !VistaDBContext.SQLChannel.IsAvailable)
        return true;
      TriggerContext triggerContext = VistaDBContext.SQLChannel.TriggerContext;
      if (triggerContext == null)
        return true;
      Table table1 = (Table) null;
      Table table2 = (Table) null;
      if (triggerContext.ModificationTables != null)
      {
        foreach (Table modificationTable in triggerContext.ModificationTables)
        {
          if (Database.DatabaseObject.EqualNames(modificationTable.Name, Table.TriggeredInsert))
            table1 = modificationTable;
          else if (Database.DatabaseObject.EqualNames(modificationTable.Name, Table.TriggeredDelete))
            table2 = modificationTable;
        }
      }
      if (table1 == null || table2 == null)
        return true;
      if (newRow != null)
      {
        for (int columnOrdinal = 0; columnOrdinal < newRow.Count; ++columnOrdinal)
        {
          if (newRow[columnOrdinal].Edited)
            triggerContext.SetUpdatedColumn(columnOrdinal);
        }
        table1.Rowset.SatelliteRow.Copy(newRow);
        table1.Rowset.CreateRow(true, false);
      }
      if (oldRow != null)
      {
        table2.Rowset.SatelliteRow.Copy(oldRow);
        table2.Rowset.CreateRow(true, false);
      }
      return true;
    }

    internal override ClusteredRowset ParentRowset
    {
      get
      {
        return this;
      }
    }

    internal override bool IsClustered
    {
      get
      {
        return true;
      }
    }

    internal override bool PostponedSynchronization
    {
      get
      {
        return false;
      }
    }

    internal override bool SuppressErrors
    {
      get
      {
        if (this.WrapperDatabase != null)
          return this.WrapperDatabase.SuppressErrors;
        return base.SuppressErrors;
      }
      set
      {
        if (this.WrapperDatabase == null)
          base.SuppressErrors = value;
        else
          this.WrapperDatabase.SuppressErrors = value;
      }
    }

    internal override bool AllowPostponing
    {
      get
      {
        return true;
      }
    }

    protected override Row OnCreateEmptyRowInstance()
    {
      return Row.CreateInstance(0U, !this.Header.Descend, this.Encryption, this.ComparingMask);
    }

    protected override Row OnCreateEmptyRowInstance(int maxColCount)
    {
      return Row.CreateInstance(0U, !this.Header.Descend, this.Encryption, this.ComparingMask, maxColCount);
    }

    internal override int DoSplitPolicy(int oldCount)
    {
      return oldCount - 1;
    }

    protected override StorageHeader DoCreateHeaderInstance(int pageSize, CultureInfo culture, DataStorage clonedStorage)
    {
      if (clonedStorage != null)
        return base.DoCreateHeaderInstance(pageSize, culture, clonedStorage);
      return (StorageHeader) ClusteredRowset.ClusteredRowsetHeader.CreateInstance((DataStorage) this, pageSize, culture);
    }

    protected override void OnDeclareNewStorage(object hint)
    {
      if (hint == null)
        throw new VistaDBException(119, this.Name);
      this.newSchema = (IVistaDBTableSchema) hint;
      if (this.WrapperDatabase == null || this.WrapperDatabase.EncryptionKey.Key != null)
        return;
      foreach (IVistaDBColumnAttributes columnAttributes in (IEnumerable<IVistaDBColumnAttributes>) this.newSchema)
        this.newSchema.DefineColumnAttributes(columnAttributes.Name, columnAttributes.AllowNull, columnAttributes.ReadOnly, false, columnAttributes.Packed, columnAttributes.Caption, columnAttributes.Description);
    }

    protected override void OnLowLevelLockRow(uint rowId)
    {
      base.OnLowLevelLockRow(rowId + (uint) byte.MaxValue);
    }

    protected override void OnLowLevelUnlockRow(uint rowId)
    {
      base.OnLowLevelUnlockRow(rowId + (uint) byte.MaxValue);
    }

    protected override TranslationList OnCreateTranslationsList(DataStorage destinationStorage)
    {
      if (this.alterList == null)
        return base.OnCreateTranslationsList(destinationStorage);
      TranslationList translationList = new TranslationList();
      foreach (AlterList.AlterInformation alterInformation in this.alterList.Values)
      {
        if (alterInformation.NewColumn != null)
        {
          if (alterInformation.OldColumn != null)
            translationList.AddTranslationRule((Row.Column) alterInformation.OldColumn, (Row.Column) alterInformation.NewColumn);
          else if (alterInformation.NewDefaults != null)
          {
            DefaultValue dstDefaults = new DefaultValue(this.Parser.Compile(alterInformation.NewColumn.Name + ":" + alterInformation.NewDefaults.Expression, destinationStorage, false), Filter.FilterType.DefaultValueInsertGenerator, true);
            translationList.AddTranslationRule(dstDefaults, (Row.Column) alterInformation.NewColumn);
          }
        }
      }
      return translationList;
    }

    protected override void OnFlushDefaultRow()
    {
      if ((int) this.Header.DefaultRowVersion == (int) this.DefaultRow.RowVersion)
        return;
      this.DefaultRow.RowId = 0U;
      this.DefaultRow.WriteExtensions((DataStorage) this, false, true);
      int memoryApartment = this.DefaultRow.GetMemoryApartment((Row) null);
      int pageSize = this.PageSize;
      int num = memoryApartment + (pageSize - memoryApartment % pageSize) % pageSize;
      if (num > this.DefaultRow.FormatLength)
      {
        int pageCount1 = this.DefaultRow.FormatLength / pageSize;
        int pageCount2 = num / pageSize;
        if ((long) this.DefaultRow.Position != (long) Row.EmptyReference && pageCount1 > 0)
          this.SetFreeCluster(this.DefaultRow.Position, pageCount1);
        this.DefaultRow.Position = this.GetFreeCluster(pageCount2);
      }
      this.DefaultRow.FormatLength = num;
      this.Header.DefaultRowPosition = this.DefaultRow.Position;
      this.Header.DefaultRowLength = num;
      this.Header.DefaultRowVersion = this.DefaultRow.RowVersion;
      this.WriteRow(this.DefaultRow);
    }

    protected override void OnCreateDefaultRow()
    {
      int index = 0;
      for (int count = this.DefaultRow.Count; index < count; ++index)
        this.DefaultRow[index].Value = (object) null;
      this.DefaultRow.RowVersion = 1U;
    }

    protected override void OnFlushStorageVersion()
    {
      this.FlushDefaultRow();
      base.OnFlushStorageVersion();
    }

    protected override void OnActivateDefaultRow()
    {
      this.DefaultRow.Position = this.Header.DefaultRowPosition;
      this.DefaultRow.FormatLength = this.Header.DefaultRowLength;
      this.ReadRow(this.DefaultRow);
    }

    protected override void OnCreateHeader(ulong position)
    {
      this.Header.CreateSchema(this.NewSchema);
      try
      {
        base.OnCreateHeader(position);
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 123, this.Name);
      }
      this.Header.RegisterSchema(this.NewSchema);
    }

    protected override void OnActivateHeader(ulong position)
    {
      base.OnActivateHeader(position);
      if (!this.Header.ActivateSchema())
        return;
      base.OnActivateHeader(position);
      if (this.Header.ActivateSchema())
        throw new VistaDBException(104, this.Name);
    }

    protected override void OnOpenStorage(StorageHandle.StorageMode openMode, ulong headerPosition)
    {
      base.OnOpenStorage(openMode, headerPosition);
      if (this.IsAlterTemporaryTable)
        return;
      this.transactionLog = this.DoOpenTpLog(this.Header.TransactionLogPosition);
      if (!this.Header.ActiveSyncColumns)
        return;
      this.originatorIndex = this.CurrentRow.LookForColumn(ClusteredRowset.SyncExtension.OriginatorIdName).RowIndex;
      this.createTimestampIndex = this.CurrentRow.LookForColumn(ClusteredRowset.SyncExtension.CreateTimestampName).RowIndex;
      this.updateTimestampIndex = this.CurrentRow.LookForColumn(ClusteredRowset.SyncExtension.UpdateTimestampName).RowIndex;
    }

    protected override void OnCreateStorage(StorageHandle.StorageMode openMode, ulong headerPosition)
    {
      base.OnCreateStorage(openMode, headerPosition);
      if (this.IsAlterTemporaryTable)
        return;
      this.transactionLog = this.DoCreateTpLog(false);
      if (!this.Header.ActiveSyncColumns)
        return;
      this.originatorIndex = this.CurrentRow.LookForColumn(ClusteredRowset.SyncExtension.OriginatorIdName).RowIndex;
      this.createTimestampIndex = this.CurrentRow.LookForColumn(ClusteredRowset.SyncExtension.CreateTimestampName).RowIndex;
      this.updateTimestampIndex = this.CurrentRow.LookForColumn(ClusteredRowset.SyncExtension.UpdateTimestampName).RowIndex;
    }

    private Table OpenTombstoneTable()
    {
      if (!this.ActiveSyncService)
        return (Table) null;
      Table table = (Table) this.WrapperDatabase.OpenClone(ClusteredRowset.SyncExtension.TombstoneTablenamePrefix + this.Name, this.IsReadOnly);
      table.Rowset.SuppressAutoValues = true;
      return table;
    }

    protected override Row DoAllocateDefaultRow()
    {
      this.lastIdentity = new ClusteredRowset.LastIdentity(this.CreateEmptyRowInstance());
      Row row = this.Header.AllocateDefaultRow(this.CreateEmptyRowInstance());
      Row.Column timeStampColumn = row.TimeStampColumn;
      if (timeStampColumn != (Row.Column) null && this.WrapperDatabase != null)
        this.WrapperDatabase.ActivateLastTimestamp(this.Name, timeStampColumn);
      return row;
    }

    protected override Row DoAllocateCurrentRow()
    {
      return this.DefaultRow.CopyInstance();
    }

    protected override bool DoBeforeCreateRow()
    {
      IVistaDBDDAEventDelegate eventDelegate = this.eventDelegates.GetDelegate(DDAEventDelegateType.BeforeInsert);
      if (eventDelegate != null)
      {
        Exception exception = eventDelegate.EventDelegate(eventDelegate, (IVistaDBRow) this.SatelliteRow.CopyInstance());
        if (exception != null)
          throw exception;
      }
      this.EnterModification();
      return true;
    }

    protected override bool DoAfterCreateRow(bool created)
    {
      try
      {
        IVistaDBDDAEventDelegate eventDelegate = this.eventDelegates.GetDelegate(DDAEventDelegateType.AfterInsert);
        if (eventDelegate != null)
        {
          Exception exception = eventDelegate.EventDelegate(eventDelegate, (IVistaDBRow) this.CurrentRow.CopyInstance());
          if (exception != null)
            throw exception;
        }
        created = base.DoAfterCreateRow(created);
        if (this.WrapperDatabase != null)
        {
          this.WrapperDatabase.SetLastIdentity(this.Name, created ? this.CurrentRow : this.DefaultRow);
          this.WrapperDatabase.SetLastTimeStamp(this.Name, created ? this.CurrentRow : this.DefaultRow);
        }
      }
      catch (Exception ex)
      {
        created = false;
        throw ex;
      }
      finally
      {
        try
        {
          this.ExitModification(!created);
        }
        finally
        {
          if (!created || !this.PostponedUserUnlock)
            this.UnlockRow(this.SatelliteRow.RowId, true, false);
        }
      }
      return created;
    }

    protected override bool DoBeforeUpdateRow(uint rowId)
    {
      this.LockRow(rowId, true);
      try
      {
        IVistaDBDDAEventDelegate eventDelegate = this.eventDelegates.GetDelegate(DDAEventDelegateType.BeforeUpdate);
        if (eventDelegate != null)
        {
          Exception exception = eventDelegate.EventDelegate(eventDelegate, (IVistaDBRow) this.SatelliteRow.CopyInstance());
          if (exception != null)
            throw exception;
        }
        this.EnterModification();
      }
      catch (Exception ex)
      {
        this.UnlockRow(rowId, true, true);
        throw ex;
      }
      return true;
    }

    protected override bool DoAfterUpdateRow(uint rowId, bool passed)
    {
      try
      {
        IVistaDBDDAEventDelegate eventDelegate = this.eventDelegates.GetDelegate(DDAEventDelegateType.AfterUpdate);
        if (eventDelegate != null)
        {
          Exception exception = eventDelegate.EventDelegate(eventDelegate, (IVistaDBRow) this.CurrentRow.CopyInstance());
          if (exception != null)
            throw exception;
        }
        passed = base.DoAfterUpdateRow(rowId, passed);
        if (this.WrapperDatabase != null)
          this.WrapperDatabase.SetLastTimeStamp(this.Name, passed ? this.CurrentRow : this.DefaultRow);
      }
      catch (Exception ex)
      {
        passed = false;
        throw ex;
      }
      finally
      {
        try
        {
          this.ExitModification(!passed);
        }
        catch (Exception ex)
        {
          passed = false;
          throw ex;
        }
        finally
        {
          if (!passed || !this.PostponedUserUnlock)
            this.UnlockRow(rowId, true, false);
        }
      }
      return passed;
    }

    protected override bool DoBeforeDeleteRow(uint rowId)
    {
      this.LockRow(rowId, true);
      try
      {
        IVistaDBDDAEventDelegate eventDelegate = this.eventDelegates.GetDelegate(DDAEventDelegateType.BeforeDelete);
        if (eventDelegate != null)
        {
          Exception exception = eventDelegate.EventDelegate(eventDelegate, (IVistaDBRow) this.SatelliteRow.CopyInstance());
          if (exception != null)
            throw exception;
        }
        this.EnterModification();
      }
      catch (Exception ex)
      {
        this.UnlockRow(rowId, true, true);
        throw ex;
      }
      return true;
    }

    protected override bool DoAfterDeleteRow(uint rowId, bool passed)
    {
      try
      {
        IVistaDBDDAEventDelegate eventDelegate = this.eventDelegates.GetDelegate(DDAEventDelegateType.AfterDelete);
        if (eventDelegate != null)
        {
          Exception exception = eventDelegate.EventDelegate(eventDelegate, (IVistaDBRow) this.CurrentRow.CopyInstance());
          if (exception != null)
            throw exception;
        }
        passed = base.DoAfterDeleteRow(rowId, passed);
      }
      catch (Exception ex)
      {
        passed = false;
        throw ex;
      }
      finally
      {
        try
        {
          this.ExitModification(!passed);
        }
        catch (Exception ex)
        {
          passed = false;
          throw ex;
        }
        finally
        {
          this.UnlockRow(rowId, true, false);
        }
      }
      return passed;
    }

    protected override bool OnCreateRow(bool blank, Row newRow)
    {
      this.DoCopyNonEdited(this.DefaultRow, newRow);
      this.DoInitCreatedRow(newRow);
      this.LockRow(newRow.RowId, true);
      this.BlockCreateGenerators(newRow);
      try
      {
        return base.OnCreateRow(blank, newRow) && this.DoCheckNulls(newRow) && (this.DoAssignIdentity(newRow) && this.DoAllocateExtensions(newRow, true)) && this.DoMirrowModifications((Row) null, newRow, TriggerAction.AfterInsert);
      }
      catch (Exception ex)
      {
        if (this.WrapperDatabase != null && this.WrapperDatabase.RepairMode)
          return true;
        this.lastIdentity.SetNulls();
        throw ex;
      }
    }

    protected override bool OnUpdateRow(Row oldRow, Row newRow)
    {
      if ((int) oldRow.RowId == (int) Row.MinRowId || (int) oldRow.RowId == (int) Row.MaxRowId)
        throw new VistaDBException(260, oldRow.RowId.ToString());
      this.TestTransactionUpdate(oldRow);
      this.DoCopyNonEdited(oldRow, newRow);
      this.InitUpdatedRow(oldRow, newRow);
      this.BlockUpdateGenerators(newRow);
      if (base.OnUpdateRow(oldRow, newRow) && this.DoCheckNulls(newRow) && this.DoReallocateExtensions(oldRow, newRow))
        return this.DoMirrowModifications(oldRow, newRow, TriggerAction.AfterUpdate);
      return false;
    }

    protected override bool OnDeleteRow(Row currentRow)
    {
      this.SaveRow();
      this.TestTransactionUpdate(currentRow);
      this.AddTransactionLogEvent(this.TransactionId);
      this.AddTombstoneRow(currentRow);
      if ((int) currentRow.RowId == (int) Row.MinRowId || (int) currentRow.RowId == (int) Row.MaxRowId)
        throw new VistaDBException(261, currentRow.RowId.ToString());
      if (this.DoMirrowModifications(currentRow, (Row) null, TriggerAction.AfterDelete) && base.OnDeleteRow(currentRow))
        return this.DoDeallocateExtensions(currentRow);
      return false;
    }

    protected override Row DoEvaluateLink(DataStorage masterStorage, EvalStack linking, Row sourceRow, Row targetRow)
    {
      targetRow.RowId = sourceRow.RowId;
      targetRow.RowVersion = sourceRow.RowVersion;
      targetRow.Position = sourceRow.RefPosition;
      return targetRow;
    }

    protected override Row OnCompileRow(string keyEvaluationExpression, bool initTop)
    {
      EvalStack evalStack = this.Parser.Compile(keyEvaluationExpression, (DataStorage) this, false);
      evalStack.Exec(this.CurrentRow, this.CreateEmptyRowInstance());
      Row evaluatedRow = evalStack.EvaluatedRow;
      if (evaluatedRow.Count == 0)
        evaluatedRow.AppendColumn((IColumn) evalStack.EvaluatedColumn);
      return evaluatedRow;
    }

    protected override void OnSynch(int asynchCounter)
    {
      this.FreezeRelationships();
      try
      {
        base.OnSynch(asynchCounter);
      }
      finally
      {
        this.DefreezeRelationships();
      }
    }

    protected override bool OnExport(DataStorage destinationStorage, bool interruptOnError)
    {
      destinationStorage.WrapperDatabase.LockStorage();
      bool commit = false;
      try
      {
        destinationStorage.LockStorage();
        if (destinationStorage.IsReadOnly)
          throw new VistaDBException(337, destinationStorage.Name);
        try
        {
          base.OnExport(destinationStorage, interruptOnError);
          this.ExportRow(this.DefaultRow, destinationStorage.DefaultRow);
          ++destinationStorage.DefaultRow.RowVersion;
          if (((ClusteredRowset) destinationStorage).SuppressAutoValues)
            destinationStorage.Header.InitTimestamp(this.Header.CurrentTimestampId);
          destinationStorage.FlushStorageVersion();
          commit = true;
          return commit;
        }
        finally
        {
          destinationStorage.UnlockStorage(false);
          destinationStorage.FinalizeChanges(!commit, commit);
        }
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 360, this.Name);
      }
      finally
      {
        destinationStorage.WrapperDatabase.UnlockStorage(false);
        destinationStorage.WrapperDatabase.FinalizeChanges(!commit, commit);
      }
    }

    protected override bool OnExportRow(Row sourceRow, Row destinationRow)
    {
      foreach (TranslationList.Rule translation in (Hashtable) this.TranslationList)
        translation.Convert(sourceRow, destinationRow, this.Culture);
      return true;
    }

    protected override void OnCommitStorageVersion()
    {
      base.OnCommitStorageVersion();
    }

    protected override void OnRollbackStorageVersion()
    {
      base.OnRollbackStorageVersion();
    }

    protected override void OnFinalizeChanges(bool rollback, bool commit)
    {
      try
      {
        base.OnFinalizeChanges(rollback, commit);
        this.triggeredRowsets.FinalizeChanges(this, rollback, commit);
        if (this.transactionLog == null)
          return;
        if (rollback && this.transactionLog.NoFileImage)
        {
          this.transactionLog.Dispose();
          this.transactionLog = (TransactionLogRowset) null;
        }
        else
          this.transactionLog.FinalizeChanges(rollback, commit);
      }
      catch
      {
        commit = false;
        rollback = true;
        throw;
      }
      finally
      {
        if (this.WrapperDatabase != null && !this.IsTransactionLogged)
          this.WrapperDatabase.FinalizeChanges(rollback, commit);
      }
    }

    protected override void Destroy()
    {
      if (this.triggeredRowsets != null)
        this.triggeredRowsets.Dispose();
      this.triggeredRowsets = (ClusteredRowset.TriggeredRowsets) null;
      if (this.eventDelegates != null)
        this.eventDelegates.Clear();
      this.eventDelegates = (ClusteredRowset.EventDelegateList) null;
      this.lastIdentity = (ClusteredRowset.LastIdentity) null;
      if (this.transactionLog != null)
        this.transactionLog.Dispose();
      this.transactionLog = (TransactionLogRowset) null;
      if (this.selfRelations != null)
        this.selfRelations.Clear();
      this.selfRelations = (InsensitiveHashtable) null;
      this.DeactivateTriggers();
      base.Destroy();
    }

    protected override void OnUpdateStorageVersion(ref bool newVersion)
    {
      base.OnUpdateStorageVersion(ref newVersion);
      if (!newVersion)
        return;
      IVistaDBDDAEventDelegate eventDelegate = this.eventDelegates.GetDelegate(DDAEventDelegateType.NewVersion);
      if (eventDelegate == null)
        return;
      Exception exception = eventDelegate.EventDelegate(eventDelegate, (IVistaDBRow) null);
    }

    protected override void OnReactivateIndex()
    {
      base.OnReactivateIndex();
      if (this.transactionLog != null)
        this.transactionLog.ReactivateIndex();
      else
        this.transactionLog = this.DoOpenTpLog(this.Header.TransactionLogPosition);
    }

    protected override void OnRereadExtendedColumn(ExtendedColumn column, Row rowKey)
    {
      Row row = (int) rowKey.RowId == (int) this.CurrentRow.RowId ? (Row) null : this.CurrentRow.CopyInstance();
      this.LockStorage();
      this.FreezeRelationships();
      try
      {
        this.MoveToRow(rowKey);
        if ((int) rowKey.RowId != (int) this.CurrentRow.RowId)
          throw new VistaDBException(262);
        try
        {
          column.UnformatExtension((DataStorage) this, false, (Row) null, false);
        }
        catch
        {
          if (this.WrapperDatabase != null && this.WrapperDatabase.RepairMode)
            return;
          throw;
        }
      }
      finally
      {
        if (row != null)
          this.MoveToRow(row);
        this.DefreezeRelationships();
        this.UnlockStorage(true);
      }
    }

    internal override uint RowCount
    {
      get
      {
        uint rowCount = base.RowCount;
        uint transactionId = this.TransactionId;
        if (transactionId == 0U || this.transactionLog == null)
          return rowCount;
        int extraRowCount = this.transactionLog.GetExtraRowCount(transactionId);
        return rowCount + (uint) extraRowCount;
      }
    }

    internal TransactionLogRowset TransactionLog
    {
      get
      {
        return this.transactionLog;
      }
    }

    internal virtual TransactionLogRowset DoCreateTpLog(bool commit)
    {
      return this.WrapperDatabase.CreateTransactionLogTable(this, commit);
    }

    internal virtual TransactionLogRowset DoOpenTpLog(ulong logHeaderPostion)
    {
      return this.WrapperDatabase.OpenTransactionLogTable(this, logHeaderPostion);
    }

    internal override TpStatus DoGettingAnotherTransactionStatus(uint transactionId)
    {
      if (transactionId != 0U && this.transactionLog != null)
        return this.transactionLog.GetTransactionStatus(transactionId);
      return TpStatus.Commit;
    }

    internal override void DoIncreaseRowCount()
    {
      uint transactionId = this.TransactionId;
      if (transactionId == 0U || this.transactionLog == null)
        base.DoIncreaseRowCount();
      else
        this.transactionLog.IncreaseRowCount(transactionId);
    }

    internal override void DoDecreaseRowCount()
    {
      uint transactionId = this.TransactionId;
      if (transactionId == 0U || this.transactionLog == null)
        base.DoDecreaseRowCount();
      else
        this.transactionLog.DecreaseRowCount(transactionId);
    }

    internal override void DoUpdateRowCount()
    {
    }

    private Database.ClrTriggerCollection GetTriggers(TriggerAction eventType)
    {
      switch (eventType)
      {
        case TriggerAction.AfterInsert:
          return this.triggersAfterInsert;
        case TriggerAction.AfterUpdate:
          return this.triggersAfterUpdate;
        case TriggerAction.AfterDelete:
          return this.triggersAfterDelete;
        default:
          return (Database.ClrTriggerCollection) null;
      }
    }

    private Database.ClrTriggerCollection GetAvailableTriggers(TriggerAction eventType)
    {
      Database.ClrTriggerCollection triggers = this.GetTriggers(eventType);
      if (triggers == null || triggers.Count == 0 || triggers.InAction)
        return (Database.ClrTriggerCollection) null;
      foreach (Database.ClrTriggerCollection.ClrTriggerInformation triggerInformation in triggers.Values)
      {
        if (triggerInformation.Active)
          return triggers;
      }
      return (Database.ClrTriggerCollection) null;
    }

    internal bool StopTriggers(TriggerAction eventType)
    {
      if (VistaDBContext.SQLChannel.IsAvailable)
        return VistaDBContext.SQLChannel.CurrentConnection.IsTriggerActing(this.Name, eventType);
      return true;
    }

    internal void PrepareTriggers(TriggerAction eventType)
    {
      VistaDBContext.SQLChannel.PushTriggerContext(this.GetAvailableTriggers(eventType) == null || this.StopTriggers(eventType) ? (Table[]) null : (Table[]) this.WrapperDatabase.ActivateModificationTable(this, eventType), eventType, this.CurrentRow.Count);
    }

    internal void ExecuteCLRTriggers(TriggerAction eventType, bool justReset)
    {
      try
      {
        if (justReset)
          return;
        Database.ClrTriggerCollection availableTriggers = this.GetAvailableTriggers(eventType);
        if (availableTriggers == null || this.StopTriggers(eventType))
          return;
        availableTriggers.InAction = true;
        try
        {
          VistaDBContext.SQLChannel.CurrentConnection.RegisterTrigger(this.Name, eventType);
          try
          {
            foreach (Database.DatabaseObject databaseObject in availableTriggers.Values)
              this.WrapperDatabase.InvokeCLRTrigger(databaseObject.Name);
          }
          finally
          {
            VistaDBContext.SQLChannel.CurrentConnection.UnregisterTrigger(this.Name, eventType);
          }
        }
        finally
        {
          availableTriggers.InAction = false;
        }
      }
      finally
      {
        VistaDBContext.SQLChannel.PopTriggerContext();
      }
    }

    internal RowIdFilter NewOptimizedFilter
    {
      get
      {
        if (this.optimizedRowFilter != null)
        {
          this.DetachFilter(Filter.FilterType.Optimized, (Filter) this.optimizedRowFilter);
          this.optimizedRowFilter = (RowIdFilter) null;
        }
        this.optimizedRowFilter = new RowIdFilter(this.Header.CurrentAutoId);
        return this.optimizedRowFilter;
      }
    }

    internal void BeginOptimizedFiltering(IOptimizedFilter filter)
    {
      if (this.optimizedRowFilter != null)
        this.optimizedRowFilter = (RowIdFilter) null;
      this.optimizedRowFilter = (RowIdFilter) filter;
      this.optimizedRowFilter.PrepareAttachment();
      this.DetachFiltersByType(Filter.FilterType.Optimized);
      this.AttachFilter((Filter) this.optimizedRowFilter);
    }

    internal void ResetOptimizedFiltering()
    {
      this.DetachFiltersByType(Filter.FilterType.Optimized);
    }

    internal bool AppendSyncStructure(Table.TableSchema schema)
    {
      if (schema.ContainsSyncPart)
        return false;
      ClusteredRowset.SyncExtension.AppendToSchema(schema);
      return true;
    }

    internal bool DeleteSyncStructure(Table.TableSchema schema)
    {
      if (!schema.ContainsSyncPart)
        return false;
      ClusteredRowset.SyncExtension.DropFromSchema(schema);
      return true;
    }

    internal bool ActiveSyncService
    {
      get
      {
        if (this.Header.ActiveSyncColumns)
          return this.type != Table.TableType.Tombstone;
        return false;
      }
    }

    internal virtual Guid Originator
    {
      get
      {
        return this.WrapperDatabase.Originator;
      }
    }

    internal class SyncExtension
    {
      internal static string TombstoneTablenamePrefix = "_syncTombstone@";
      internal static string TombstoneTableDescription = "Collection of deleted rows for table ";
      internal static string OriginatorIdName = "_syncOriginatorId";
      internal static string UpdateTimestampName = "_syncUpdateTimestamp";
      internal static string CreateTimestampName = "_syncCreateTimestamp";
      internal static string AnchorTablename = "_syncAnchor";
      internal static string AnchorTableDescription = "Collection of sync anchors";
      internal static string SyncTableName = "_syncTable";
      internal static string LastReceivedAnchorName = "_syncReceivedAnchor";
      internal static string LastSentAnchorName = "_syncSentAnchor";
      private static SyncOriginator originator = new SyncOriginator();
      private static SyncTimestamp updateTimestamp = new SyncTimestamp();
      private static SyncTimestamp createTimestamp = new SyncTimestamp();

      static SyncExtension()
      {
        ClusteredRowset.SyncExtension.originator.AssignAttributes(ClusteredRowset.SyncExtension.OriginatorIdName, false, false, false, false);
        ClusteredRowset.SyncExtension.updateTimestamp.AssignAttributes(ClusteredRowset.SyncExtension.UpdateTimestampName, false, true, false, false);
        ClusteredRowset.SyncExtension.createTimestamp.AssignAttributes(ClusteredRowset.SyncExtension.CreateTimestampName, false, true, false, false);
      }

      internal static void AppendToSchema(Table.TableSchema schema)
      {
        schema.AddSyncColumn(ClusteredRowset.SyncExtension.originator.Duplicate(false));
        schema.AddSyncColumn(ClusteredRowset.SyncExtension.updateTimestamp.Duplicate(false));
        schema.AddSyncColumn(ClusteredRowset.SyncExtension.createTimestamp.Duplicate(false));
      }

      internal static void DropFromSchema(Table.TableSchema schema)
      {
        schema.DropSyncColumn(ClusteredRowset.SyncExtension.OriginatorIdName);
        schema.DropSyncColumn(ClusteredRowset.SyncExtension.UpdateTimestampName);
        schema.DropSyncColumn(ClusteredRowset.SyncExtension.CreateTimestampName);
      }

      internal static IVistaDBTableSchema GetAnchorSchema(Database db)
      {
        Table.TableSchema tableSchema = new Table.TableSchema(ClusteredRowset.SyncExtension.AnchorTablename, Table.TableType.Anchor, ClusteredRowset.SyncExtension.AnchorTableDescription, 0UL, (DataStorage) db);
        tableSchema.AddColumn(ClusteredRowset.SyncExtension.SyncTableName, VistaDBType.NVarChar, 1024, 0, false);
        tableSchema.AddColumn(ClusteredRowset.SyncExtension.OriginatorIdName, VistaDBType.UniqueIdentifier, 0, 0, false);
        tableSchema.AddColumn(ClusteredRowset.SyncExtension.LastReceivedAnchorName, VistaDBType.VarBinary, 0, 0, false);
        tableSchema.AddColumn(ClusteredRowset.SyncExtension.LastSentAnchorName, VistaDBType.VarBinary, 0, 0, false);
        return (IVistaDBTableSchema) tableSchema;
      }
    }

    private class EventDelegateList : Dictionary<DDAEventDelegateType, IVistaDBDDAEventDelegate>
    {
      internal void SetDelegate(IVistaDBDDAEventDelegate eventDelegate)
      {
        this.ResetDelegate(eventDelegate.Type);
        this.Add(eventDelegate.Type, eventDelegate);
      }

      internal void ResetDelegate(DDAEventDelegateType type)
      {
        if (!this.ContainsKey(type))
          return;
        this.Remove(type);
      }

      internal IVistaDBDDAEventDelegate GetDelegate(DDAEventDelegateType type)
      {
        if (!this.ContainsKey(type))
          return (IVistaDBDDAEventDelegate) null;
        return this[type];
      }
    }

    private class TriggeredRowsets : List<Table>, IDisposable
    {
      private ClusteredRowset parentRowset;
      private bool active;
      private bool isDisposed;

      internal void FinalizeChanges(ClusteredRowset firingRowset, bool rollback, bool commit)
      {
        if (this.active || this.Count == 0 || !commit && !rollback)
          return;
        this.active = true;
        try
        {
          for (int index = 0; index < this.Count; ++index)
          {
            Table table = this[index];
            if (table != null && table.Rowset != null)
            {
              ClusteredRowset rowset = table.Rowset;
              if (rowset != firingRowset)
              {
                rowset.FinalizeChanges(rollback, commit);
                if (!rowset.postponedClosing)
                  this.Remove(table);
              }
            }
          }
        }
        finally
        {
          this.active = false;
        }
      }

      internal void CloseTriggered()
      {
        try
        {
          for (int index = 0; index < this.Count; ++index)
          {
            Table table = this[index];
            if (table != null && table.Rowset != null && table.Rowset.postponedClosing)
              table.Dispose();
          }
        }
        finally
        {
          this.Clear();
        }
      }

      internal TriggeredRowsets(ClusteredRowset parentRowset)
      {
        this.parentRowset = parentRowset;
      }

      internal void AddFiredTable(Table triggeredTable, bool postponedClosing)
      {
        if (triggeredTable.Rowset == this.parentRowset || this.Contains(triggeredTable))
          return;
        this.Add(triggeredTable);
        triggeredTable.Rowset.postponedClosing = postponedClosing;
      }

      public void Dispose()
      {
        if (this.isDisposed)
          return;
        this.isDisposed = true;
        GC.SuppressFinalize((object) this);
        this.FinalizeChanges(this.parentRowset, true, false);
        this.parentRowset = (ClusteredRowset) null;
        this.Clear();
      }
    }

    internal class ClusteredRowsetHeader : VistaDB.Engine.Core.Indexing.Index.IndexHeader
    {
      private int defaultRowLengthIndex;
      private int defaultRowVersionIndex;
      private bool activeSyncColumns;

      internal static ClusteredRowset.ClusteredRowsetHeader CreateInstance(DataStorage parentStorage, int pageSize, CultureInfo culture)
      {
        return new ClusteredRowset.ClusteredRowsetHeader(parentStorage, VistaDB.Engine.Core.Header.HeaderId.ROWSET_HEADER, VistaDB.Engine.Core.Indexing.Index.Type.Clustered, pageSize, culture);
      }

      protected ClusteredRowsetHeader(DataStorage parentStorage, VistaDB.Engine.Core.Header.HeaderId id, VistaDB.Engine.Core.Indexing.Index.Type type, int pageSize, CultureInfo culture)
        : base(parentStorage, id, type, pageSize, culture)
      {
        this.defaultRowLengthIndex = this.AppendColumn((IColumn) new IntColumn(0));
        this.defaultRowVersionIndex = this.AppendColumn((IColumn) new IntColumn(0));
      }

      internal int DefaultRowLength
      {
        get
        {
          return (int) this[this.defaultRowLengthIndex].Value;
        }
        set
        {
          this.Modified = this.DefaultRowLength != value;
          this[this.defaultRowLengthIndex].Value = (object) value;
        }
      }

      internal uint DefaultRowVersion
      {
        get
        {
          return (uint) (int) this[this.defaultRowVersionIndex].Value;
        }
        set
        {
          this.Modified = (int) this.DefaultRowVersion != (int) value;
          this[this.defaultRowVersionIndex].Value = (object) (int) value;
        }
      }

      internal ulong DefaultRowPosition
      {
        get
        {
          return this.RefPosition;
        }
        set
        {
          this.RefPosition = value;
        }
      }

      internal bool ActiveSyncColumns
      {
        get
        {
          return this.activeSyncColumns;
        }
      }

      internal Row AllocateDefaultRow(Row rowInstance)
      {
        return this.OnAllocateDefaultRow(rowInstance);
      }

      protected virtual Row OnAllocateDefaultRow(Row rowInstance)
      {
        try
        {
          Row row = this.ParentStorage.WrapperDatabase.AllocateRowsetSchema(this.ParentStorage.StorageId, rowInstance);
          this.activeSyncColumns = row[row.Count - 1].IsSync;
          return row;
        }
        finally
        {
          rowInstance.InstantiateComparingMask();
        }
      }
    }

    private class LastIdentity
    {
      protected Row identValue;

      internal LastIdentity(Row emptyRow)
      {
        this.identValue = emptyRow;
        this.identValue.Clear();
      }

      internal Row Value
      {
        get
        {
          return this.identValue.CopyInstance();
        }
      }

      private void Resort()
      {
        int count = this.identValue.Count;
        Row.Column column = this.identValue[count - 1];
        int index1 = count - 1;
        for (int index2 = index1 - 1; index2 >= 0 && this.identValue[index2].RowIndex >= column.RowIndex; --index2)
          index1 = index2;
        this.identValue.Insert(index1, column);
        this.identValue.RemoveAt(count);
      }

      internal Row GetTableIdentity(Row defaultRow)
      {
        Row row = this.identValue.CopyInstance();
        int index = 0;
        for (int count = row.Count; index < count; ++index)
          row[index].Value = defaultRow[this.identValue[index].RowIndex].Value;
        return row;
      }

      internal void AddColumn(Row.Column column)
      {
        Row.Column column1 = column.Duplicate(false);
        this.identValue.AppendColumn((IColumn) column1);
        column1.RowIndex = column.RowIndex;
        this.Resort();
      }

      internal void DropColumn(Row.Column column)
      {
        int index = 0;
        for (int count = this.identValue.Count; index < count; ++index)
        {
          if (this.identValue[index].RowIndex == column.RowIndex)
          {
            this.identValue.RemoveAt(index);
            break;
          }
        }
      }

      internal void SetNulls()
      {
        foreach (Row.Column column in (List<Row.Column>) this.identValue)
          column.Value = (object) null;
      }

      internal void AssignValue(Row newValue)
      {
        foreach (Row.Column column in (List<Row.Column>) this.identValue)
          column.Value = newValue[column.RowIndex].Value;
      }
    }
  }
}
