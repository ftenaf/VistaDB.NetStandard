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
    internal class ClusteredRowset : Index
    {
        private static int rowsetRunTimeId = 0;
        internal static string TemporaryName = "$$${0}_tmp";
        internal static ulong ModificationCounter = 0;
        private int originatorIndex = -1;
        private int createTimestampIndex = -1;
        private int updateTimestampIndex = -1;
        private IVistaDBTableSchema newSchema;
        private LastIdentity lastIdentity;
        private bool rowLockPostponed;
        private TriggeredRowsets triggeredRowsets;
        private EventDelegateList eventDelegates;
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
            ++rowsetRunTimeId;
            string alias = "TCR" + rowsetRunTimeId.ToString();
            ClusteredRowset clusteredRowset = new ClusteredRowset(tableName, alias, parentDatabase, parentDatabase.ParentConnection, parentDatabase.Parser, parentDatabase.Encryption, clonedOrigin, type);
            clusteredRowset.DoAfterConstruction(parentDatabase.PageSize, parentDatabase.Culture);
            return clusteredRowset;
        }

        protected ClusteredRowset(string tableName, string alias, Database parentDatabase, DirectConnection connection, Parser parser, Encryption encryption, ClusteredRowset clonedOrigin, Table.TableType type)
          : base(tableName, alias, parser, connection, parentDatabase, encryption, (Index)clonedOrigin)
        {
            triggeredRowsets = new TriggeredRowsets(this);
            eventDelegates = new EventDelegateList();
            this.type = type;
        }

        internal IVistaDBTableSchema NewSchema
        {
            get
            {
                return newSchema;
            }
        }

        new internal ClusteredRowsetHeader Header
        {
            get
            {
                return (ClusteredRowsetHeader)base.Header;
            }
        }

        internal Row LastSessionIdentity
        {
            get
            {
                return lastIdentity.Value;
            }
        }

        internal Row LastTableIdentity
        {
            get
            {
                LockStorage();
                try
                {
                    return lastIdentity.GetTableIdentity(DefaultRow);
                }
                finally
                {
                    UnlockStorage(true);
                }
            }
        }

        internal bool IsSystemTable
        {
            get
            {
                return type != Table.TableType.Default;
            }
        }

        internal bool IsAlterTemporaryTable
        {
            get
            {
                if (newSchema != null)
                    return ((Table.TableSchema)newSchema).TemporarySchema;
                return false;
            }
        }

        internal bool AllowSyncEdit
        {
            get
            {
                return allowSyncEdit;
            }
            set
            {
                allowSyncEdit = value;
            }
        }

        internal bool PostponedUserUnlock
        {
            get
            {
                return rowLockPostponed;
            }
            set
            {
                rowLockPostponed = value;
            }
        }

        internal bool PostponedClosing
        {
            get
            {
                return postponedClosing;
            }
        }

        internal AlterList AlterList
        {
            get
            {
                return alterList;
            }
            set
            {
                alterList = value;
            }
        }

        internal uint MaxRowId
        {
            get
            {
                return Header.CurrentAutoId;
            }
        }

        internal bool SuppressAutoValues
        {
            get
            {
                return suppressAutoValues;
            }
            set
            {
                suppressAutoValues = value;
            }
        }

        private int[] ComparingMask
        {
            get
            {
                if (DefaultRow != null)
                    return DefaultRow.ComparingMask;
                return (int[])null;
            }
        }

        private void EnterModification()
        {
            LockStorage();
        }

        private void ExitModification(bool instantly)
        {
            UnlockStorage(instantly);
        }

        private void InitUpdatedRow(Row oldRow, Row newRow)
        {
            newRow.CopyMetaData(oldRow);
            newRow.RowVersion = TransactionId;
            if (!SuppressAutoValues)
            {
                if (oldRow.HasTimestamp)
                    newRow.SetTimestamp(Header.NextTimestampId);
                if (ActiveSyncService)
                {
                    newRow.SetTimestamp(Header.NextTimestampId, updateTimestampIndex);
                    newRow.SetOriginator(Originator, originatorIndex, !AllowSyncEdit);
                }
            }
            AddTransactionLogEvent(TransactionId);
        }

        private void AddTransactionLogEvent(uint transactionId)
        {
            if (transactionId == 0U)
                return;
            if (transactionLog == null)
                transactionLog = DoCreateTpLog(false);
            transactionLog.RegisterTransaction(false, transactionId);
        }

        private void AddTombstoneRow(Row oldRow)
        {
            Table triggeredTable = OpenTombstoneTable();
            if (triggeredTable == null)
                return;
            ClusteredRowset rowset = triggeredTable.Rowset;
            rowset.SatelliteRow.Copy(oldRow);
            rowset.SatelliteRow[rowset.updateTimestampIndex].Value = (object)(long)Header.NextTimestampId;
            rowset.SatelliteRow[rowset.originatorIndex].Value = (object)Originator;
            rowset.CreateRow(false, false);
            if (WrapperDatabase == null)
                return;
            WrapperDatabase.AddTriggeredDependence(triggeredTable, !triggeredTable.IsClone);
        }

        private void TestTransactionUpdate(Row oldRow)
        {
            if (transactionLog == null)
                return;
            uint transactionId = oldRow.TransactionId;
            if ((int)transactionId != (int)TransactionId && oldRow.OutdatedStatus && DoGettingAnotherTransactionStatus(transactionId) == TpStatus.Active)
                throw new VistaDBException(455);
        }

        internal void UpdateSchemaVersion()
        {
            ++Header.SchemaVersion;
            FlushStorageVersion();
        }

        internal void ActivateIdentity(Row.Column column, string step)
        {
            EvalStack evaluation = Parser.Compile(Identity.SystemName + "(" + column.Name + "," + step + ")", (DataStorage)this, true);
            DeactivateIdentity(column);
            DeactivateDefaultValue(column);
            lastIdentity.AddColumn(column);
            AttachFilter((Filter)new Identity(evaluation));
            ActivateReadOnly(column);
            WrapperDatabase.ActivateLastIdentity(Name, column);
        }

        internal void DeactivateIdentity(Row.Column column)
        {
            DetachIdentityFilter(column);
            if (!column.ReadOnly)
                DeactivateReadOnly(column);
            lastIdentity.DropColumn(column);
        }

        internal void CreateIdentity(string columnName, string seedExpression, string stepExpression)
        {
            WrapperDatabase.LockStorage();
            bool commit = true;
            try
            {
                LockStorage();
                if (IsReadOnly)
                    throw new VistaDBException(337, Name);
                try
                {
                    Row.Column column1 = LookForColumn(columnName);
                    if (column1 == (Row.Column)null)
                        throw new VistaDBException(181, columnName);
                    switch (column1.Type)
                    {
                        case VistaDBType.SmallInt:
                        case VistaDBType.Int:
                        case VistaDBType.BigInt:
                            Row.Column column2 = seedExpression == null ? DefaultRow[column1.RowIndex].Duplicate(false) : CompileRow(seedExpression, true)[0];
                            Row.Column column3 = CompileRow(stepExpression, true)[0];
                            if (column2.IsNull)
                                throw new VistaDBException(188, seedExpression);
                            if (column3.IsNull)
                                throw new VistaDBException(189, stepExpression);
                            try
                            {
                                switch (column1.Type)
                                {
                                    case VistaDBType.SmallInt:
                                        int num = (int)short.Parse(stepExpression);
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
                            Conversion.Convert((IValue)column2, (IValue)DefaultRow[column1.RowIndex]);
                            ++DefaultRow.RowVersion;
                            ActivateIdentity(column1, column3.Value.ToString());
                            WrapperDatabase.RegisterIdentity(this, column1.Name, stepExpression);
                            commit = true;
                            break;
                        default:
                            throw new VistaDBException(191, column1.Name);
                    }
                }
                catch (Exception)
                {
                    WrapperDatabase.ReactivateObjects(this);
                    throw;
                }
                finally
                {
                    UnlockStorage(false);
                    FinalizeChanges(!commit, commit);
                }
            }
            catch (Exception ex)
            {
                throw new VistaDBException(ex, 186, columnName);
            }
            finally
            {
                WrapperDatabase.UnlockStorage(false);
                WrapperDatabase.FinalizeChanges(!commit, commit);
            }
        }

        internal void DropIdentity(string columnName)
        {
            WrapperDatabase.LockStorage();
            bool commit = false;
            try
            {
                if (IsReadOnly)
                    throw new VistaDBException(337, Name);
                LockStorage();
                try
                {
                    Row.Column column = LookForColumn(columnName);
                    if (column == (Row.Column)null)
                        throw new VistaDBException(181, columnName);
                    DefaultRow[column.RowIndex].Value = (object)null;
                    ++DefaultRow.RowVersion;
                    WrapperDatabase.UnregisterIdentity(this, column.Name, true);
                    DeactivateIdentity(column);
                    commit = true;
                }
                finally
                {
                    UnlockStorage(false);
                    FinalizeChanges(!commit, commit);
                }
            }
            catch (Exception ex)
            {
                throw new VistaDBException(ex, 187, columnName);
            }
            finally
            {
                WrapperDatabase.UnlockStorage(false);
                WrapperDatabase.FinalizeChanges(!commit, commit);
            }
        }

        internal void ActivateDefaultValue(Row.Column column, string scriptExpression, bool useInUpdate)
        {
            scriptExpression = column.Name + ":" + scriptExpression;
            try
            {
                EvalStack evaluation = Parser.Compile(scriptExpression, (DataStorage)this, true);
                DeactivateDefaultValue(column);
                if (useInUpdate)
                    AttachFilter((Filter)new DefaultValue(evaluation, Filter.FilterType.DefaultValueUpdateGenerator, false));
                AttachFilter((Filter)new DefaultValue(evaluation, Filter.FilterType.DefaultValueInsertGenerator, true));
            }
            catch (VistaDBException)
            {
            }
            catch (Exception)
            {
            }
        }

        internal void DeactivateDefaultValue(Row.Column column)
        {
            DetachDefaultValueFilter(column);
        }

        internal void CreateDefaultValue(string columnName, string scriptExpression, bool useInUpdate, string description)
        {
            WrapperDatabase.LockStorage();
            bool commit = false;
            try
            {
                LockStorage();
                if (IsReadOnly)
                    throw new VistaDBException(337, Name);
                try
                {
                    Row.Column column = LookForColumn(columnName);
                    if (column == (Row.Column)null)
                        throw new VistaDBException(181, columnName);
                    switch (column.Type)
                    {
                        case VistaDBType.Image:
                        case VistaDBType.VarBinary:
                        case VistaDBType.Timestamp:
                            throw new VistaDBException(198, column.Name);
                        default:
                            if (WrapperDatabase.IsIdentityRegistered(this, column.Name))
                                throw new VistaDBException(184, column.Name);
                            if (column.InternalType == VistaDBType.NChar)
                            {
                                string strB = scriptExpression.Trim(' ', char.MinValue, '(', ')');
                                if (strB.Length > 1 && strB[0] != '\'' && (strB[strB.Length - 1] != '\'' && string.Compare("NULL", strB, StringComparison.OrdinalIgnoreCase) != 0))
                                    scriptExpression = "'" + strB.Replace("'", "''") + "'";
                            }
                            EvalStack evalStack = useInUpdate ? Parser.Compile(scriptExpression, (DataStorage)this, false) : (EvalStack)null;
                            if (evalStack != null && evalStack.IsConstantResult)
                            {
                                Conversion.Convert((IValue)CompileRow(scriptExpression, true)[0], (IValue)DefaultRow[column.RowIndex]);
                                ++DefaultRow.RowVersion;
                                if (WrapperDatabase.IsDefaultValueRegistered(this, column.Name))
                                {
                                    WrapperDatabase.UnregisterDefaultValue(this, column.Name, false);
                                    DeactivateDefaultValue(column);
                                }
                            }
                            else
                            {
                                DefaultRow[column.RowIndex].Value = (object)null;
                                ++DefaultRow.RowVersion;
                                ActivateDefaultValue(column, scriptExpression, useInUpdate);
                                WrapperDatabase.RegisterDefaultValue(this, column.Name, scriptExpression, useInUpdate, description);
                            }
                            commit = true;
                            break;
                    }
                }
                catch (Exception ex)
                {
                    WrapperDatabase.ReactivateObjects(this);
                    throw ex;
                }
                finally
                {
                    UnlockStorage(false);
                    FinalizeChanges(!commit, commit);
                }
            }
            catch (Exception ex)
            {
                throw new VistaDBException(ex, 193, columnName);
            }
            finally
            {
                WrapperDatabase.UnlockStorage(false);
                WrapperDatabase.FinalizeChanges(!commit, commit);
            }
        }

        internal void DropDefaultValue(string columnName)
        {
            WrapperDatabase.LockStorage();
            bool commit = false;
            try
            {
                if (IsReadOnly)
                    throw new VistaDBException(337, Name);
                LockStorage();
                try
                {
                    Row.Column column = LookForColumn(columnName);
                    if (column == (Row.Column)null)
                        throw new VistaDBException(181, columnName);
                    DefaultRow[column.RowIndex].Value = (object)null;
                    ++DefaultRow.RowVersion;
                    WrapperDatabase.UnregisterDefaultValue(this, column.Name, true);
                    DeactivateDefaultValue(column);
                    commit = true;
                }
                finally
                {
                    UnlockStorage(false);
                    FinalizeChanges(!commit, commit);
                }
            }
            catch (Exception ex)
            {
                throw new VistaDBException(ex, 194, columnName);
            }
            finally
            {
                WrapperDatabase.UnlockStorage(false);
                WrapperDatabase.FinalizeChanges(!commit, commit);
            }
        }

        internal void ActivateConstraint(string name, string expression, int options, DataStorage activeOrder, bool foreignKeyConstraint)
        {
            if (options == 0)
                return;
            DeactivateConstraint(name);
            CheckStatement checkConstraint = (CheckStatement)null;
            if (!foreignKeyConstraint)
            {
                checkConstraint = WrapperDatabase.SQLContext.CreateCheckConstraint(expression, Name, DefaultRow);
                int num = (int)checkConstraint.PrepareQuery();
            }
            if (Constraint.InsertionActivity(options))
                AttachFilter(foreignKeyConstraint ? (Filter)new FKConstraint(name, Parser.Compile(expression, activeOrder, true), Filter.FilterType.ConstraintAppend) : (Filter)new SQLConstraint(name, Filter.FilterType.ConstraintAppend, checkConstraint, expression));
            if (Constraint.UpdateActivity(options))
                AttachFilter(foreignKeyConstraint ? (Filter)new FKConstraint(name, Parser.Compile(expression, activeOrder, true), Filter.FilterType.ConstraintUpdate) : (Filter)new SQLConstraint(name, Filter.FilterType.ConstraintUpdate, checkConstraint, expression));
            if (!Constraint.DeleteActivity(options))
                return;
            AttachFilter(foreignKeyConstraint ? (Filter)new FKConstraint(name, Parser.Compile(expression, activeOrder, true), Filter.FilterType.ConstraintDelete) : (Filter)new SQLConstraint(name, Filter.FilterType.ConstraintDelete, checkConstraint, expression));
        }

        internal void DeactivateConstraint(string name)
        {
            DetachConstraintFilter(name);
        }

        internal void CreateConstraint(string name, string scriptExpression, string description, bool insertion, bool update, bool delete)
        {
            WrapperDatabase.LockStorage();
            bool commit = false;
            Exception exception = (Exception)null;
            try
            {
                LockStorage();
                if (IsReadOnly)
                    throw new VistaDBException(337, Name);
                try
                {
                    int options = Constraint.MakeStatus(insertion, update, delete);
                    ActivateConstraint(name, scriptExpression, options, (DataStorage)this, false);
                    WrapperDatabase.RegisterConstraint(this, name, scriptExpression, options, description);
                    commit = true;
                }
                catch (Exception ex)
                {
                    exception = ex;
                    WrapperDatabase.ReactivateObjects(this);
                    throw ex;
                }
                finally
                {
                    UnlockStorage(false);
                    FinalizeChanges(!commit, commit);
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
                WrapperDatabase.UnlockStorage(false);
                WrapperDatabase.FinalizeChanges(!commit, commit);
            }
        }

        internal void DropConstraint(string name)
        {
            WrapperDatabase.LockStorage();
            bool commit = false;
            try
            {
                if (IsReadOnly)
                    throw new VistaDBException(337, Name);
                LockStorage();
                try
                {
                    WrapperDatabase.UnregisterConstraint(this, name, true);
                    DeactivateConstraint(name);
                    commit = true;
                }
                finally
                {
                    UnlockStorage(false);
                    FinalizeChanges(!commit, commit);
                }
            }
            catch (Exception ex)
            {
                throw new VistaDBException(ex, 197, name);
            }
            finally
            {
                WrapperDatabase.UnlockStorage(false);
                WrapperDatabase.FinalizeChanges(!commit, commit);
            }
        }

        internal void UpdateIndexInformation(RowsetIndex index, IVistaDBIndexInformation newIndex, bool commit)
        {
            WrapperDatabase.LockStorage();
            bool flag = false;
            try
            {
                if (IsReadOnly)
                    throw new VistaDBException(337, Name);
                LockStorage();
                try
                {
                    WrapperDatabase.UpdateRegisteredIndex(index, newIndex, commit);
                    flag = true;
                }
                finally
                {
                    UnlockStorage(false);
                    FinalizeChanges(!flag, commit);
                }
            }
            catch (Exception ex)
            {
                throw new VistaDBException(ex, 131, index.Alias);
            }
            finally
            {
                WrapperDatabase.UnlockStorage(false);
                WrapperDatabase.FinalizeChanges(!flag, commit);
            }
        }

        internal void DropIndex(RowsetIndex index, bool commit)
        {
            WrapperDatabase.LockStorage();
            bool flag = false;
            try
            {
                if (IsReadOnly)
                    throw new VistaDBException(337, Name);
                if (index.IsForeignKey)
                    throw new VistaDBException(132, index.Alias);
                LockStorage();
                try
                {
                    WrapperDatabase.UnregisterIndex(index);
                    flag = true;
                }
                finally
                {
                    UnlockStorage(false);
                    FinalizeChanges(!flag, commit);
                }
            }
            catch (Exception ex)
            {
                throw new VistaDBException(ex, 131, index.Alias);
            }
            finally
            {
                WrapperDatabase.UnlockStorage(false);
                WrapperDatabase.FinalizeChanges(!flag, commit);
            }
        }

        internal void RenameIndex(RowsetIndex index, string oldName, string newName, bool commit)
        {
            WrapperDatabase.LockStorage();
            bool flag = false;
            try
            {
                if (IsReadOnly)
                    throw new VistaDBException(337, Name);
                if (index.IsForeignKey)
                    throw new VistaDBException(159, index.Alias);
                LockStorage();
                try
                {
                    WrapperDatabase.UnregisterIndex(index);
                    index.Alias = newName;
                    WrapperDatabase.RegisterIndex(index);
                    flag = true;
                }
                finally
                {
                    UnlockStorage(false);
                    FinalizeChanges(!flag, commit);
                }
            }
            catch (Exception ex)
            {
                throw new VistaDBException(ex, 131, index.Alias);
            }
            finally
            {
                WrapperDatabase.UnlockStorage(false);
                WrapperDatabase.FinalizeChanges(!flag, commit);
            }
        }

        internal void ActivateReadOnly(Row.Column column)
        {
            EvalStack evaluation = Parser.Compile(Readonly.SystemName + "(" + column.Name + ")", (DataStorage)this, true);
            DeactivateReadOnly(column);
            AttachFilter((Filter)new Readonly(column.Name, evaluation));
        }

        internal void DeactivateReadOnly(Row.Column column)
        {
            DetachReadonlyFilter(column.Name);
        }

        internal void CreateForeignKey(Table parentFkTable, string constraintName, string foreignKey, string primaryTable, VistaDBReferentialIntegrity updateIntegrity, VistaDBReferentialIntegrity deleteIntegrity, string description)
        {
            Table table = (Table)null;
            WrapperDatabase.LockStorage();
            bool commit = false;
            try
            {
                if (IsReadOnly)
                    throw new VistaDBException(337, Name);
                LockStorage();
                try
                {
                    if (!WrapperDatabase.ContainsPrimaryKey(primaryTable))
                        throw new VistaDBException(202, primaryTable);
                    EvalStack fkEvaluator = Parser.Compile(foreignKey, (DataStorage)this, false);
                    ClusteredRowset rowset = parentFkTable.Rowset;
                    table = (Table)WrapperDatabase.OpenClone(primaryTable, rowset.IsReadOnly);
                    foreach (Index index in table.Values)
                    {
                        if (index.IsPrimary)
                        {
                            if (!index.DoCheckIfRelated(fkEvaluator))
                                throw new VistaDBException(203, foreignKey);
                            if (Database.DatabaseObject.EqualNames(table.Name, Name))
                            {
                                if (index.DoCheckIfSame(fkEvaluator))
                                    throw new VistaDBException(206, foreignKey);
                                break;
                            }
                            break;
                        }
                    }
                    parentFkTable.CreateIndex(constraintName, foreignKey, (string)null, false, false, false, false, true, false, false);
                    WrapperDatabase.RegisterForeignKey(constraintName, this, table.Rowset, foreignKey, updateIntegrity, deleteIntegrity, description);
                    ActivateForeignKey(parentFkTable, constraintName, table.Rowset.Name);
                    commit = true;
                }
                finally
                {
                    UnlockStorage(false);
                    FinalizeChanges(!commit, commit);
                }
            }
            catch (Exception ex)
            {
                throw new VistaDBException(ex, 200, constraintName);
            }
            finally
            {
                WrapperDatabase.UnlockStorage(false);
                WrapperDatabase.FinalizeChanges(!commit, commit);
                WrapperDatabase.ReleaseClone((ITable)table);
            }
        }

        internal void DropForeignKey(Table parentFkTable, RowsetIndex foreignIndex, string constraintName, bool commit)
        {
            WrapperDatabase.LockStorage();
            bool flag = false;
            try
            {
                if (IsReadOnly)
                    throw new VistaDBException(337, Name);
                LockStorage();
                try
                {
                    WrapperDatabase.UnregisterForeignKey(constraintName, this);
                    if (foreignIndex.IsForeignKey)
                        WrapperDatabase.UnregisterIndex(foreignIndex);
                    DeactivateForeignKey(parentFkTable, constraintName);
                    flag = true;
                }
                finally
                {
                    UnlockStorage(false);
                    FinalizeChanges(!flag, commit);
                }
            }
            catch (Exception ex)
            {
                throw new VistaDBException(ex, 201, constraintName);
            }
            finally
            {
                WrapperDatabase.UnlockStorage(false);
                WrapperDatabase.FinalizeChanges(!flag, commit);
            }
        }

        internal void ActivateForeignKey(Table fkTable, string fkIndexName, string pkTableName)
        {
            Index indexOrder = fkTable.GetIndexOrder(fkIndexName, true);
            string keyExpression = ((RowsetIndex)indexOrder).KeyExpression;
            ActivateConstraint(fkTable.Rowset.Name + "." + fkIndexName, ForeignKeyConstraint.ReferencedKey + "( '" + pkTableName + "' )", Constraint.MakeStatus(true, true, false), (DataStorage)indexOrder, true);
        }

        internal void DeactivateForeignKey(Table fkTable, string fkIndexName)
        {
            DeactivateConstraint(fkTable.Rowset.Name + "." + fkIndexName);
        }

        internal void ActivatePrimaryKeyReference(Table pkTable, string fkTableName, string fkIndexName, VistaDBReferentialIntegrity updateIntegrity, VistaDBReferentialIntegrity deleteIntegrity)
        {
            Index indexOrder = pkTable.GetIndexOrder(pkTable.PKIndex, true);
            ActivateConstraint(pkTable.Rowset.Name + ".update." + fkIndexName, NonreferencedPrimaryKey.NonReferencedKey + "('" + fkTableName + "','" + fkIndexName + "'," + ((int)updateIntegrity).ToString() + ")", Constraint.MakeStatus(false, true, false), (DataStorage)indexOrder, true);
            ActivateConstraint(pkTable.Rowset.Name + ".delete." + fkIndexName, NonreferencedPrimaryKey.NonReferencedKey + "('" + fkTableName + "','" + fkIndexName + "'," + ((int)deleteIntegrity).ToString() + ")", Constraint.MakeStatus(false, false, true), (DataStorage)indexOrder, true);
        }

        internal void DeactivatePrimaryKeyReference(Table pkTable, string fkIndexName)
        {
            DeactivateConstraint(pkTable.Rowset.Name + ".update." + fkIndexName);
            DeactivateConstraint(pkTable.Rowset.Name + ".delete." + fkIndexName);
        }

        internal void AddTriggeredDependence(Table triggeredTable, bool closeByFinalization)
        {
            triggeredRowsets.AddFiredTable(triggeredTable, closeByFinalization);
        }

        internal void SetDelegate(IVistaDBDDAEventDelegate eventDelegate)
        {
            eventDelegates.SetDelegate(eventDelegate);
        }

        internal void ResetDelegate(DDAEventDelegateType type)
        {
            eventDelegates.ResetDelegate(type);
        }

        internal void CloseTriggeredTables()
        {
            triggeredRowsets.CloseTriggered();
        }

        internal void SaveSelfRelationship(IVistaDBRelationshipInformation relationship)
        {
            if (selfRelations == null)
                selfRelations = new InsensitiveHashtable();
            if (selfRelations.ContainsKey((object)relationship.Name))
                return;
            selfRelations.Add((object)relationship.Name, (object)relationship);
        }

        internal void FreezeSelfRelationships(Table table)
        {
            if (selfRelations == null)
                return;
            foreach (IVistaDBRelationshipInformation selfRelation in (Hashtable)selfRelations)
            {
                DeactivatePrimaryKeyReference(table, selfRelation.Name);
                DeactivateForeignKey(table, selfRelation.Name);
            }
        }

        internal void DefreezeSelfRelationships(Table table)
        {
            if (selfRelations == null)
                return;
            foreach (IVistaDBRelationshipInformation selfRelation in (Hashtable)selfRelations)
            {
                ActivatePrimaryKeyReference(table, Name, selfRelation.Name, selfRelation.UpdateIntegrity, selfRelation.DeleteIntegrity);
                ActivateForeignKey(table, selfRelation.Name, Name);
            }
        }

        internal void ActivateTriggers(IVistaDBTableSchema schema)
        {
            foreach (Database.ClrTriggerCollection.ClrTriggerInformation trigger in (IEnumerable<IVistaDBClrTriggerInformation>)schema.Triggers)
            {
                if (trigger.Active)
                {
                    if ((((IVistaDBClrTriggerInformation)trigger).TriggerAction & TriggerAction.AfterInsert) == TriggerAction.AfterInsert)
                    {
                        if (triggersAfterInsert == null)
                            triggersAfterInsert = new Database.ClrTriggerCollection();
                        triggersAfterInsert.AddTrigger(trigger);
                    }
                    if ((((IVistaDBClrTriggerInformation)trigger).TriggerAction & TriggerAction.AfterUpdate) == TriggerAction.AfterUpdate)
                    {
                        if (triggersAfterUpdate == null)
                            triggersAfterUpdate = new Database.ClrTriggerCollection();
                        triggersAfterUpdate.AddTrigger(trigger);
                    }
                    if ((((IVistaDBClrTriggerInformation)trigger).TriggerAction & TriggerAction.AfterDelete) == TriggerAction.AfterDelete)
                    {
                        if (triggersAfterDelete == null)
                            triggersAfterDelete = new Database.ClrTriggerCollection();
                        triggersAfterDelete.AddTrigger(trigger);
                    }
                }
            }
        }

        internal void DeactivateTriggers()
        {
            if (triggersAfterInsert != null)
                triggersAfterInsert.Clear();
            triggersAfterInsert = (Database.ClrTriggerCollection)null;
            if (triggersAfterUpdate != null)
                triggersAfterUpdate.Clear();
            triggersAfterUpdate = (Database.ClrTriggerCollection)null;
            if (triggersAfterDelete != null)
                triggersAfterDelete.Clear();
            triggersAfterDelete = (Database.ClrTriggerCollection)null;
        }

        protected virtual void DoCopyNonEdited(Row sourceRow, Row destinRow)
        {
            if (SuppressAutoValues)
                return;
            bool flag = sourceRow == DefaultRow;
            int index = 0;
            for (int count = destinRow.Count; index < count; ++index)
            {
                Row.Column column = destinRow[index];
                if (column.ExtendedType)
                    ((ExtendedColumn)column).ResetMetaValue();
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
            newRow.RowId = Header.AutoId;
            newRow.RowVersion = TransactionId;
            newRow.RefPosition = Row.EmptyReference;
            if (!SuppressAutoValues)
            {
                if (newRow.HasTimestamp)
                    newRow.SetTimestamp(Header.NextTimestampId);
                if (ActiveSyncService)
                {
                    ulong nextTimestampId = Header.NextTimestampId;
                    newRow.SetTimestamp(nextTimestampId, createTimestampIndex);
                    newRow.SetTimestamp(nextTimestampId, updateTimestampIndex);
                    newRow.SetOriginator(Originator, originatorIndex, !AllowSyncEdit);
                }
            }
            AddTransactionLogEvent(TransactionId);
        }

        protected virtual bool DoAssignIdentity(Row newRow)
        {
            lastIdentity.AssignValue(newRow);
            return true;
        }

        protected virtual bool DoCheckNulls(Row row)
        {
            foreach (Row.Column column in (List<Row.Column>)row)
            {
                if (!column.AllowNull && column.IsNull)
                    throw new VistaDBException(174, column.Name);
            }
            return true;
        }

        protected virtual bool DoAllocateExtensions(Row newRow, bool fresh)
        {
            newRow.WriteExtensions((DataStorage)this, fresh, true);
            return true;
        }

        protected virtual bool DoDeallocateExtensions(Row oldRow)
        {
            if (!IsTransaction)
                return oldRow.FreeExtensionSpace((DataStorage)this);
            return true;
        }

        protected virtual bool DoReallocateExtensions(Row oldRow, Row newRow)
        {
            if (IsTransaction)
                return DoAllocateExtensions(newRow, false);
            RowExtension extensions = newRow.Extensions;
            if (extensions == null)
                return true;
            foreach (ExtendedColumn extendedColumn in (List<IColumn>)extensions)
            {
                if (extendedColumn.NeedFlush)
                    ((ExtendedColumn)oldRow[extendedColumn.RowIndex]).FreeSpace((DataStorage)this);
            }
            newRow.WriteExtensions((DataStorage)this, false, true);
            return true;
        }

        protected virtual bool DoMirrowModifications(Row oldRow, Row newRow, TriggerAction triggerAction)
        {
            if (GetAvailableTriggers(triggerAction) == null || !VistaDBContext.SQLChannel.IsAvailable)
                return true;
            TriggerContext triggerContext = VistaDBContext.SQLChannel.TriggerContext;
            if (triggerContext == null)
                return true;
            Table table1 = (Table)null;
            Table table2 = (Table)null;
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
                if (WrapperDatabase != null)
                    return WrapperDatabase.SuppressErrors;
                return base.SuppressErrors;
            }
            set
            {
                if (WrapperDatabase == null)
                    base.SuppressErrors = value;
                else
                    WrapperDatabase.SuppressErrors = value;
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
            return Row.CreateInstance(0U, !Header.Descend, Encryption, ComparingMask);
        }

        protected override Row OnCreateEmptyRowInstance(int maxColCount)
        {
            return Row.CreateInstance(0U, !Header.Descend, Encryption, ComparingMask, maxColCount);
        }

        internal override int DoSplitPolicy(int oldCount)
        {
            return oldCount - 1;
        }

        protected override StorageHeader DoCreateHeaderInstance(int pageSize, CultureInfo culture, DataStorage clonedStorage)
        {
            if (clonedStorage != null)
                return base.DoCreateHeaderInstance(pageSize, culture, clonedStorage);
            return (StorageHeader)ClusteredRowsetHeader.CreateInstance((DataStorage)this, pageSize, culture);
        }

        protected override void OnDeclareNewStorage(object hint)
        {
            if (hint == null)
                throw new VistaDBException(119, Name);
            newSchema = (IVistaDBTableSchema)hint;
            if (WrapperDatabase == null || WrapperDatabase.EncryptionKey.Key != null)
                return;
            foreach (IVistaDBColumnAttributes columnAttributes in (IEnumerable<IVistaDBColumnAttributes>)newSchema)
                newSchema.DefineColumnAttributes(columnAttributes.Name, columnAttributes.AllowNull, columnAttributes.ReadOnly, false, columnAttributes.Packed, columnAttributes.Caption, columnAttributes.Description);
        }

        protected override void OnLowLevelLockRow(uint rowId)
        {
            base.OnLowLevelLockRow(rowId + (uint)byte.MaxValue);
        }

        protected override void OnLowLevelUnlockRow(uint rowId)
        {
            base.OnLowLevelUnlockRow(rowId + (uint)byte.MaxValue);
        }

        protected override TranslationList OnCreateTranslationsList(DataStorage destinationStorage)
        {
            if (alterList == null)
                return base.OnCreateTranslationsList(destinationStorage);
            TranslationList translationList = new TranslationList();
            foreach (AlterList.AlterInformation alterInformation in alterList.Values)
            {
                if (alterInformation.NewColumn != null)
                {
                    if (alterInformation.OldColumn != null)
                        translationList.AddTranslationRule((Row.Column)alterInformation.OldColumn, (Row.Column)alterInformation.NewColumn);
                    else if (alterInformation.NewDefaults != null)
                    {
                        DefaultValue dstDefaults = new DefaultValue(Parser.Compile(alterInformation.NewColumn.Name + ":" + alterInformation.NewDefaults.Expression, destinationStorage, false), Filter.FilterType.DefaultValueInsertGenerator, true);
                        translationList.AddTranslationRule(dstDefaults, (Row.Column)alterInformation.NewColumn);
                    }
                }
            }
            return translationList;
        }

        protected override void OnFlushDefaultRow()
        {
            if ((int)Header.DefaultRowVersion == (int)DefaultRow.RowVersion)
                return;
            DefaultRow.RowId = 0U;
            DefaultRow.WriteExtensions((DataStorage)this, false, true);
            int memoryApartment = DefaultRow.GetMemoryApartment((Row)null);
            int pageSize = PageSize;
            int num = memoryApartment + (pageSize - memoryApartment % pageSize) % pageSize;
            if (num > DefaultRow.FormatLength)
            {
                int pageCount1 = DefaultRow.FormatLength / pageSize;
                int pageCount2 = num / pageSize;
                if ((long)DefaultRow.Position != (long)Row.EmptyReference && pageCount1 > 0)
                    SetFreeCluster(DefaultRow.Position, pageCount1);
                DefaultRow.Position = GetFreeCluster(pageCount2);
            }
            DefaultRow.FormatLength = num;
            Header.DefaultRowPosition = DefaultRow.Position;
            Header.DefaultRowLength = num;
            Header.DefaultRowVersion = DefaultRow.RowVersion;
            WriteRow(DefaultRow);
        }

        protected override void OnCreateDefaultRow()
        {
            int index = 0;
            for (int count = DefaultRow.Count; index < count; ++index)
                DefaultRow[index].Value = (object)null;
            DefaultRow.RowVersion = 1U;
        }

        protected override void OnFlushStorageVersion()
        {
            FlushDefaultRow();
            base.OnFlushStorageVersion();
        }

        protected override void OnActivateDefaultRow()
        {
            DefaultRow.Position = Header.DefaultRowPosition;
            DefaultRow.FormatLength = Header.DefaultRowLength;
            ReadRow(DefaultRow);
        }

        protected override void OnCreateHeader(ulong position)
        {
            Header.CreateSchema(NewSchema);
            try
            {
                base.OnCreateHeader(position);
            }
            catch (Exception ex)
            {
                throw new VistaDBException(ex, 123, Name);
            }
            Header.RegisterSchema(NewSchema);
        }

        protected override void OnActivateHeader(ulong position)
        {
            base.OnActivateHeader(position);
            if (!Header.ActivateSchema())
                return;
            base.OnActivateHeader(position);
            if (Header.ActivateSchema())
                throw new VistaDBException(104, Name);
        }

        protected override void OnOpenStorage(StorageHandle.StorageMode openMode, ulong headerPosition)
        {
            base.OnOpenStorage(openMode, headerPosition);
            if (IsAlterTemporaryTable)
                return;
            transactionLog = DoOpenTpLog(Header.TransactionLogPosition);
            if (!Header.ActiveSyncColumns)
                return;
            originatorIndex = CurrentRow.LookForColumn(SyncExtension.OriginatorIdName).RowIndex;
            createTimestampIndex = CurrentRow.LookForColumn(SyncExtension.CreateTimestampName).RowIndex;
            updateTimestampIndex = CurrentRow.LookForColumn(SyncExtension.UpdateTimestampName).RowIndex;
        }

        protected override void OnCreateStorage(StorageHandle.StorageMode openMode, ulong headerPosition)
        {
            base.OnCreateStorage(openMode, headerPosition);
            if (IsAlterTemporaryTable)
                return;
            transactionLog = DoCreateTpLog(false);
            if (!Header.ActiveSyncColumns)
                return;
            originatorIndex = CurrentRow.LookForColumn(SyncExtension.OriginatorIdName).RowIndex;
            createTimestampIndex = CurrentRow.LookForColumn(SyncExtension.CreateTimestampName).RowIndex;
            updateTimestampIndex = CurrentRow.LookForColumn(SyncExtension.UpdateTimestampName).RowIndex;
        }

        private Table OpenTombstoneTable()
        {
            if (!ActiveSyncService)
                return (Table)null;
            Table table = (Table)WrapperDatabase.OpenClone(SyncExtension.TombstoneTablenamePrefix + Name, IsReadOnly);
            table.Rowset.SuppressAutoValues = true;
            return table;
        }

        protected override Row DoAllocateDefaultRow()
        {
            lastIdentity = new LastIdentity(CreateEmptyRowInstance());
            Row row = Header.AllocateDefaultRow(CreateEmptyRowInstance());
            Row.Column timeStampColumn = row.TimeStampColumn;
            if (timeStampColumn != (Row.Column)null && WrapperDatabase != null)
                WrapperDatabase.ActivateLastTimestamp(Name, timeStampColumn);
            return row;
        }

        protected override Row DoAllocateCurrentRow()
        {
            return DefaultRow.CopyInstance();
        }

        protected override bool DoBeforeCreateRow()
        {
            IVistaDBDDAEventDelegate eventDelegate = eventDelegates.GetDelegate(DDAEventDelegateType.BeforeInsert);
            if (eventDelegate != null)
            {
                Exception exception = eventDelegate.EventDelegate(eventDelegate, (IVistaDBRow)SatelliteRow.CopyInstance());
                if (exception != null)
                    throw exception;
            }
            EnterModification();
            return true;
        }

        protected override bool DoAfterCreateRow(bool created)
        {
            try
            {
                IVistaDBDDAEventDelegate eventDelegate = eventDelegates.GetDelegate(DDAEventDelegateType.AfterInsert);
                if (eventDelegate != null)
                {
                    Exception exception = eventDelegate.EventDelegate(eventDelegate, (IVistaDBRow)CurrentRow.CopyInstance());
                    if (exception != null)
                        throw exception;
                }
                created = base.DoAfterCreateRow(created);
                if (WrapperDatabase != null)
                {
                    WrapperDatabase.SetLastIdentity(Name, created ? CurrentRow : DefaultRow);
                    WrapperDatabase.SetLastTimeStamp(Name, created ? CurrentRow : DefaultRow);
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
                    ExitModification(!created);
                }
                finally
                {
                    if (!created || !PostponedUserUnlock)
                        UnlockRow(SatelliteRow.RowId, true, false);
                }
            }
            return created;
        }

        protected override bool DoBeforeUpdateRow(uint rowId)
        {
            LockRow(rowId, true);
            try
            {
                IVistaDBDDAEventDelegate eventDelegate = eventDelegates.GetDelegate(DDAEventDelegateType.BeforeUpdate);
                if (eventDelegate != null)
                {
                    Exception exception = eventDelegate.EventDelegate(eventDelegate, (IVistaDBRow)SatelliteRow.CopyInstance());
                    if (exception != null)
                        throw exception;
                }
                EnterModification();
            }
            catch (Exception ex)
            {
                UnlockRow(rowId, true, true);
                throw ex;
            }
            return true;
        }

        protected override bool DoAfterUpdateRow(uint rowId, bool passed)
        {
            try
            {
                IVistaDBDDAEventDelegate eventDelegate = eventDelegates.GetDelegate(DDAEventDelegateType.AfterUpdate);
                if (eventDelegate != null)
                {
                    Exception exception = eventDelegate.EventDelegate(eventDelegate, (IVistaDBRow)CurrentRow.CopyInstance());
                    if (exception != null)
                        throw exception;
                }
                passed = base.DoAfterUpdateRow(rowId, passed);
                if (WrapperDatabase != null)
                    WrapperDatabase.SetLastTimeStamp(Name, passed ? CurrentRow : DefaultRow);
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
                    ExitModification(!passed);
                }
                catch (Exception ex)
                {
                    passed = false;
                    throw ex;
                }
                finally
                {
                    if (!passed || !PostponedUserUnlock)
                        UnlockRow(rowId, true, false);
                }
            }
            return passed;
        }

        protected override bool DoBeforeDeleteRow(uint rowId)
        {
            LockRow(rowId, true);
            try
            {
                IVistaDBDDAEventDelegate eventDelegate = eventDelegates.GetDelegate(DDAEventDelegateType.BeforeDelete);
                if (eventDelegate != null)
                {
                    Exception exception = eventDelegate.EventDelegate(eventDelegate, (IVistaDBRow)SatelliteRow.CopyInstance());
                    if (exception != null)
                        throw exception;
                }
                EnterModification();
            }
            catch (Exception ex)
            {
                UnlockRow(rowId, true, true);
                throw ex;
            }
            return true;
        }

        protected override bool DoAfterDeleteRow(uint rowId, bool passed)
        {
            try
            {
                IVistaDBDDAEventDelegate eventDelegate = eventDelegates.GetDelegate(DDAEventDelegateType.AfterDelete);
                if (eventDelegate != null)
                {
                    Exception exception = eventDelegate.EventDelegate(eventDelegate, (IVistaDBRow)CurrentRow.CopyInstance());
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
                    ExitModification(!passed);
                }
                catch (Exception ex)
                {
                    passed = false;
                    throw ex;
                }
                finally
                {
                    UnlockRow(rowId, true, false);
                }
            }
            return passed;
        }

        protected override bool OnCreateRow(bool blank, Row newRow)
        {
            DoCopyNonEdited(DefaultRow, newRow);
            DoInitCreatedRow(newRow);
            LockRow(newRow.RowId, true);
            BlockCreateGenerators(newRow);
            try
            {
                return base.OnCreateRow(blank, newRow) && DoCheckNulls(newRow) && (DoAssignIdentity(newRow) && DoAllocateExtensions(newRow, true)) && DoMirrowModifications((Row)null, newRow, TriggerAction.AfterInsert);
            }
            catch (Exception ex)
            {
                if (WrapperDatabase != null && WrapperDatabase.RepairMode)
                    return true;
                lastIdentity.SetNulls();
                throw ex;
            }
        }

        protected override bool OnUpdateRow(Row oldRow, Row newRow)
        {
            if ((int)oldRow.RowId == (int)Row.MinRowId || (int)oldRow.RowId == (int)Row.MaxRowId)
                throw new VistaDBException(260, oldRow.RowId.ToString());
            TestTransactionUpdate(oldRow);
            DoCopyNonEdited(oldRow, newRow);
            InitUpdatedRow(oldRow, newRow);
            BlockUpdateGenerators(newRow);
            if (base.OnUpdateRow(oldRow, newRow) && DoCheckNulls(newRow) && DoReallocateExtensions(oldRow, newRow))
                return DoMirrowModifications(oldRow, newRow, TriggerAction.AfterUpdate);
            return false;
        }

        protected override bool OnDeleteRow(Row currentRow)
        {
            SaveRow();
            TestTransactionUpdate(currentRow);
            AddTransactionLogEvent(TransactionId);
            AddTombstoneRow(currentRow);
            if ((int)currentRow.RowId == (int)Row.MinRowId || (int)currentRow.RowId == (int)Row.MaxRowId)
                throw new VistaDBException(261, currentRow.RowId.ToString());
            if (DoMirrowModifications(currentRow, (Row)null, TriggerAction.AfterDelete) && base.OnDeleteRow(currentRow))
                return DoDeallocateExtensions(currentRow);
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
            EvalStack evalStack = Parser.Compile(keyEvaluationExpression, (DataStorage)this, false);
            evalStack.Exec(CurrentRow, CreateEmptyRowInstance());
            Row evaluatedRow = evalStack.EvaluatedRow;
            if (evaluatedRow.Count == 0)
                evaluatedRow.AppendColumn((IColumn)evalStack.EvaluatedColumn);
            return evaluatedRow;
        }

        protected override void OnSynch(int asynchCounter)
        {
            FreezeRelationships();
            try
            {
                base.OnSynch(asynchCounter);
            }
            finally
            {
                DefreezeRelationships();
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
                    ExportRow(DefaultRow, destinationStorage.DefaultRow);
                    ++destinationStorage.DefaultRow.RowVersion;
                    if (((ClusteredRowset)destinationStorage).SuppressAutoValues)
                        destinationStorage.Header.InitTimestamp(Header.CurrentTimestampId);
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
                throw new VistaDBException(ex, 360, Name);
            }
            finally
            {
                destinationStorage.WrapperDatabase.UnlockStorage(false);
                destinationStorage.WrapperDatabase.FinalizeChanges(!commit, commit);
            }
        }

        protected override bool OnExportRow(Row sourceRow, Row destinationRow)
        {
            foreach (TranslationList.Rule translation in (Hashtable)TranslationList)
                translation.Convert(sourceRow, destinationRow, Culture);
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
                triggeredRowsets.FinalizeChanges(this, rollback, commit);
                if (transactionLog == null)
                    return;
                if (rollback && transactionLog.NoFileImage)
                {
                    transactionLog.Dispose();
                    transactionLog = (TransactionLogRowset)null;
                }
                else
                    transactionLog.FinalizeChanges(rollback, commit);
            }
            catch
            {
                commit = false;
                rollback = true;
                throw;
            }
            finally
            {
                if (WrapperDatabase != null && !IsTransactionLogged)
                    WrapperDatabase.FinalizeChanges(rollback, commit);
            }
        }

        protected override void Destroy()
        {
            if (triggeredRowsets != null)
                triggeredRowsets.Dispose();
            triggeredRowsets = (TriggeredRowsets)null;
            if (eventDelegates != null)
                eventDelegates.Clear();
            eventDelegates = (EventDelegateList)null;
            lastIdentity = (LastIdentity)null;
            if (transactionLog != null)
                transactionLog.Dispose();
            transactionLog = (TransactionLogRowset)null;
            if (selfRelations != null)
                selfRelations.Clear();
            selfRelations = (InsensitiveHashtable)null;
            DeactivateTriggers();
            base.Destroy();
        }

        protected override void OnUpdateStorageVersion(ref bool newVersion)
        {
            base.OnUpdateStorageVersion(ref newVersion);
            if (!newVersion)
                return;
            IVistaDBDDAEventDelegate eventDelegate = eventDelegates.GetDelegate(DDAEventDelegateType.NewVersion);
            if (eventDelegate == null)
                return;
            Exception exception = eventDelegate.EventDelegate(eventDelegate, (IVistaDBRow)null);
        }

        protected override void OnReactivateIndex()
        {
            base.OnReactivateIndex();
            if (transactionLog != null)
                transactionLog.ReactivateIndex();
            else
                transactionLog = DoOpenTpLog(Header.TransactionLogPosition);
        }

        protected override void OnRereadExtendedColumn(ExtendedColumn column, Row rowKey)
        {
            Row row = (int)rowKey.RowId == (int)CurrentRow.RowId ? (Row)null : CurrentRow.CopyInstance();
            LockStorage();
            FreezeRelationships();
            try
            {
                MoveToRow(rowKey);
                if ((int)rowKey.RowId != (int)CurrentRow.RowId)
                    throw new VistaDBException(262);
                try
                {
                    column.UnformatExtension((DataStorage)this, false, (Row)null, false);
                }
                catch
                {
                    if (WrapperDatabase != null && WrapperDatabase.RepairMode)
                        return;
                    throw;
                }
            }
            finally
            {
                if (row != null)
                    MoveToRow(row);
                DefreezeRelationships();
                UnlockStorage(true);
            }
        }

        internal override uint RowCount
        {
            get
            {
                uint rowCount = base.RowCount;
                uint transactionId = TransactionId;
                if (transactionId == 0U || transactionLog == null)
                    return rowCount;
                int extraRowCount = transactionLog.GetExtraRowCount(transactionId);
                return rowCount + (uint)extraRowCount;
            }
        }

        internal TransactionLogRowset TransactionLog
        {
            get
            {
                return transactionLog;
            }
        }

        internal virtual TransactionLogRowset DoCreateTpLog(bool commit)
        {
            return WrapperDatabase.CreateTransactionLogTable(this, commit);
        }

        internal virtual TransactionLogRowset DoOpenTpLog(ulong logHeaderPostion)
        {
            return WrapperDatabase.OpenTransactionLogTable(this, logHeaderPostion);
        }

        internal override TpStatus DoGettingAnotherTransactionStatus(uint transactionId)
        {
            if (transactionId != 0U && transactionLog != null)
                return transactionLog.GetTransactionStatus(transactionId);
            return TpStatus.Commit;
        }

        internal override void DoIncreaseRowCount()
        {
            uint transactionId = TransactionId;
            if (transactionId == 0U || transactionLog == null)
                base.DoIncreaseRowCount();
            else
                transactionLog.IncreaseRowCount(transactionId);
        }

        internal override void DoDecreaseRowCount()
        {
            uint transactionId = TransactionId;
            if (transactionId == 0U || transactionLog == null)
                base.DoDecreaseRowCount();
            else
                transactionLog.DecreaseRowCount(transactionId);
        }

        internal override void DoUpdateRowCount()
        {
        }

        private Database.ClrTriggerCollection GetTriggers(TriggerAction eventType)
        {
            switch (eventType)
            {
                case TriggerAction.AfterInsert:
                    return triggersAfterInsert;
                case TriggerAction.AfterUpdate:
                    return triggersAfterUpdate;
                case TriggerAction.AfterDelete:
                    return triggersAfterDelete;
                default:
                    return (Database.ClrTriggerCollection)null;
            }
        }

        private Database.ClrTriggerCollection GetAvailableTriggers(TriggerAction eventType)
        {
            Database.ClrTriggerCollection triggers = GetTriggers(eventType);
            if (triggers == null || triggers.Count == 0 || triggers.InAction)
                return (Database.ClrTriggerCollection)null;
            foreach (Database.ClrTriggerCollection.ClrTriggerInformation triggerInformation in triggers.Values)
            {
                if (triggerInformation.Active)
                    return triggers;
            }
            return (Database.ClrTriggerCollection)null;
        }

        internal bool StopTriggers(TriggerAction eventType)
        {
            if (VistaDBContext.SQLChannel.IsAvailable)
                return VistaDBContext.SQLChannel.CurrentConnection.IsTriggerActing(Name, eventType);
            return true;
        }

        internal void PrepareTriggers(TriggerAction eventType)
        {
            VistaDBContext.SQLChannel.PushTriggerContext(GetAvailableTriggers(eventType) == null || StopTriggers(eventType) ? (Table[])null : (Table[])WrapperDatabase.ActivateModificationTable(this, eventType), eventType, CurrentRow.Count);
        }

        internal void ExecuteCLRTriggers(TriggerAction eventType, bool justReset)
        {
            try
            {
                if (justReset)
                    return;
                Database.ClrTriggerCollection availableTriggers = GetAvailableTriggers(eventType);
                if (availableTriggers == null || StopTriggers(eventType))
                    return;
                availableTriggers.InAction = true;
                try
                {
                    VistaDBContext.SQLChannel.CurrentConnection.RegisterTrigger(Name, eventType);
                    try
                    {
                        foreach (Database.DatabaseObject databaseObject in availableTriggers.Values)
                            WrapperDatabase.InvokeCLRTrigger(databaseObject.Name);
                    }
                    finally
                    {
                        VistaDBContext.SQLChannel.CurrentConnection.UnregisterTrigger(Name, eventType);
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
                if (optimizedRowFilter != null)
                {
                    DetachFilter(Filter.FilterType.Optimized, (Filter)optimizedRowFilter);
                    optimizedRowFilter = (RowIdFilter)null;
                }
                optimizedRowFilter = new RowIdFilter(Header.CurrentAutoId);
                return optimizedRowFilter;
            }
        }

        internal void BeginOptimizedFiltering(IOptimizedFilter filter)
        {
            if (optimizedRowFilter != null)
                optimizedRowFilter = (RowIdFilter)null;
            optimizedRowFilter = (RowIdFilter)filter;
            optimizedRowFilter.PrepareAttachment();
            DetachFiltersByType(Filter.FilterType.Optimized);
            AttachFilter((Filter)optimizedRowFilter);
        }

        internal void ResetOptimizedFiltering()
        {
            DetachFiltersByType(Filter.FilterType.Optimized);
        }

        internal bool AppendSyncStructure(Table.TableSchema schema)
        {
            if (schema.ContainsSyncPart)
                return false;
            SyncExtension.AppendToSchema(schema);
            return true;
        }

        internal bool DeleteSyncStructure(Table.TableSchema schema)
        {
            if (!schema.ContainsSyncPart)
                return false;
            SyncExtension.DropFromSchema(schema);
            return true;
        }

        internal bool ActiveSyncService
        {
            get
            {
                if (Header.ActiveSyncColumns)
                    return type != Table.TableType.Tombstone;
                return false;
            }
        }

        internal virtual Guid Originator
        {
            get
            {
                return WrapperDatabase.Originator;
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
                originator.AssignAttributes(OriginatorIdName, false, false, false, false);
                updateTimestamp.AssignAttributes(UpdateTimestampName, false, true, false, false);
                createTimestamp.AssignAttributes(CreateTimestampName, false, true, false, false);
            }

            internal static void AppendToSchema(Table.TableSchema schema)
            {
                schema.AddSyncColumn(originator.Duplicate(false));
                schema.AddSyncColumn(updateTimestamp.Duplicate(false));
                schema.AddSyncColumn(createTimestamp.Duplicate(false));
            }

            internal static void DropFromSchema(Table.TableSchema schema)
            {
                schema.DropSyncColumn(OriginatorIdName);
                schema.DropSyncColumn(UpdateTimestampName);
                schema.DropSyncColumn(CreateTimestampName);
            }

            internal static IVistaDBTableSchema GetAnchorSchema(Database db)
            {
                Table.TableSchema tableSchema = new Table.TableSchema(AnchorTablename, Table.TableType.Anchor, AnchorTableDescription, 0UL, (DataStorage)db);
                tableSchema.AddColumn(SyncTableName, VistaDBType.NVarChar, 1024, 0, false);
                tableSchema.AddColumn(OriginatorIdName, VistaDBType.UniqueIdentifier, 0, 0, false);
                tableSchema.AddColumn(LastReceivedAnchorName, VistaDBType.VarBinary, 0, 0, false);
                tableSchema.AddColumn(LastSentAnchorName, VistaDBType.VarBinary, 0, 0, false);
                return (IVistaDBTableSchema)tableSchema;
            }
        }

        private class EventDelegateList : Dictionary<DDAEventDelegateType, IVistaDBDDAEventDelegate>
        {
            internal void SetDelegate(IVistaDBDDAEventDelegate eventDelegate)
            {
                ResetDelegate(eventDelegate.Type);
                Add(eventDelegate.Type, eventDelegate);
            }

            internal void ResetDelegate(DDAEventDelegateType type)
            {
                if (!ContainsKey(type))
                    return;
                Remove(type);
            }

            internal IVistaDBDDAEventDelegate GetDelegate(DDAEventDelegateType type)
            {
                if (!ContainsKey(type))
                    return (IVistaDBDDAEventDelegate)null;
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
                if (active || Count == 0 || !commit && !rollback)
                    return;
                active = true;
                try
                {
                    for (int index = 0; index < Count; ++index)
                    {
                        Table table = this[index];
                        if (table != null && table.Rowset != null)
                        {
                            ClusteredRowset rowset = table.Rowset;
                            if (rowset != firingRowset)
                            {
                                rowset.FinalizeChanges(rollback, commit);
                                if (!rowset.postponedClosing)
                                    Remove(table);
                            }
                        }
                    }
                }
                finally
                {
                    active = false;
                }
            }

            internal void CloseTriggered()
            {
                try
                {
                    for (int index = 0; index < Count; ++index)
                    {
                        Table table = this[index];
                        if (table != null && table.Rowset != null && table.Rowset.postponedClosing)
                            table.Dispose();
                    }
                }
                finally
                {
                    Clear();
                }
            }

            internal TriggeredRowsets(ClusteredRowset parentRowset)
            {
                this.parentRowset = parentRowset;
            }

            internal void AddFiredTable(Table triggeredTable, bool postponedClosing)
            {
                if (triggeredTable.Rowset == parentRowset || Contains(triggeredTable))
                    return;
                Add(triggeredTable);
                triggeredTable.Rowset.postponedClosing = postponedClosing;
            }

            public void Dispose()
            {
                if (isDisposed)
                    return;
                isDisposed = true;
                GC.SuppressFinalize((object)this);
                FinalizeChanges(parentRowset, true, false);
                parentRowset = (ClusteredRowset)null;
                Clear();
            }
        }

        internal class ClusteredRowsetHeader : IndexHeader
        {
            private int defaultRowLengthIndex;
            private int defaultRowVersionIndex;
            private bool activeSyncColumns;

            internal static ClusteredRowsetHeader CreateInstance(DataStorage parentStorage, int pageSize, CultureInfo culture)
            {
                return new ClusteredRowsetHeader(parentStorage, HeaderId.ROWSET_HEADER, Type.Clustered, pageSize, culture);
            }

            protected ClusteredRowsetHeader(DataStorage parentStorage, HeaderId id, Type type, int pageSize, CultureInfo culture)
              : base(parentStorage, id, type, pageSize, culture)
            {
                defaultRowLengthIndex = AppendColumn((IColumn)new IntColumn(0));
                defaultRowVersionIndex = AppendColumn((IColumn)new IntColumn(0));
            }

            internal int DefaultRowLength
            {
                get
                {
                    return (int)this[defaultRowLengthIndex].Value;
                }
                set
                {
                    Modified = DefaultRowLength != value;
                    this[defaultRowLengthIndex].Value = (object)value;
                }
            }

            internal uint DefaultRowVersion
            {
                get
                {
                    return (uint)(int)this[defaultRowVersionIndex].Value;
                }
                set
                {
                    Modified = (int)DefaultRowVersion != (int)value;
                    this[defaultRowVersionIndex].Value = (object)(int)value;
                }
            }

            internal ulong DefaultRowPosition
            {
                get
                {
                    return RefPosition;
                }
                set
                {
                    RefPosition = value;
                }
            }

            internal bool ActiveSyncColumns
            {
                get
                {
                    return activeSyncColumns;
                }
            }

            internal Row AllocateDefaultRow(Row rowInstance)
            {
                return OnAllocateDefaultRow(rowInstance);
            }

            protected virtual Row OnAllocateDefaultRow(Row rowInstance)
            {
                try
                {
                    Row row = ParentStorage.WrapperDatabase.AllocateRowsetSchema(ParentStorage.StorageId, rowInstance);
                    activeSyncColumns = row[row.Count - 1].IsSync;
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
                identValue = emptyRow;
                identValue.Clear();
            }

            internal Row Value
            {
                get
                {
                    return identValue.CopyInstance();
                }
            }

            private void Resort()
            {
                int count = identValue.Count;
                Row.Column column = identValue[count - 1];
                int index1 = count - 1;
                for (int index2 = index1 - 1; index2 >= 0 && identValue[index2].RowIndex >= column.RowIndex; --index2)
                    index1 = index2;
                identValue.Insert(index1, column);
                identValue.RemoveAt(count);
            }

            internal Row GetTableIdentity(Row defaultRow)
            {
                Row row = identValue.CopyInstance();
                int index = 0;
                for (int count = row.Count; index < count; ++index)
                    row[index].Value = defaultRow[identValue[index].RowIndex].Value;
                return row;
            }

            internal void AddColumn(Row.Column column)
            {
                Row.Column column1 = column.Duplicate(false);
                identValue.AppendColumn((IColumn)column1);
                column1.RowIndex = column.RowIndex;
                Resort();
            }

            internal void DropColumn(Row.Column column)
            {
                int index = 0;
                for (int count = identValue.Count; index < count; ++index)
                {
                    if (identValue[index].RowIndex == column.RowIndex)
                    {
                        identValue.RemoveAt(index);
                        break;
                    }
                }
            }

            internal void SetNulls()
            {
                foreach (Row.Column column in (List<Row.Column>)identValue)
                    column.Value = (object)null;
            }

            internal void AssignValue(Row newValue)
            {
                foreach (Row.Column column in (List<Row.Column>)identValue)
                    column.Value = newValue[column.RowIndex].Value;
            }
        }
    }
}
