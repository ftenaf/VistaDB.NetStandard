using System;
using System.Collections.Generic;
using System.Data;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;
using VistaDB.Provider;

namespace VistaDB.Engine.SQL
{
    internal class BatchStatement : Statement
    {
        protected StatementCollection statements = new StatementCollection();
        protected Dictionary<string, IParameter> prms = new Dictionary<string, IParameter>(StringComparer.OrdinalIgnoreCase);
        protected Dictionary<string, CreateTableStatement> tempTables = new Dictionary<string, CreateTableStatement>(StringComparer.OrdinalIgnoreCase);
        private bool cascadeReturnParam = true;
        private IParameter returnParameter;
        protected int currentStatement;
        protected bool breakBatch;
        protected bool breakScope;

        public BatchStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
          : base(connection, parent, parser, id)
        {
        }

        protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
        {
        }

        protected override VistaDBType OnPrepareQuery()
        {
            foreach (Statement statement in (List<Statement>)statements)
            {
                int num = (int)statement.PrepareQuery();
                hasDDL = hasDDL || statement.HasDDLCommands;
            }
            return VistaDBType.Unknown;
        }

        public override void ResetResult()
        {
            currentStatement = 0;
            breakBatch = false;
            foreach (Statement statement in (List<Statement>)statements)
                statement.ResetResult();
        }

        public override IQueryStatement SubQuery(int index)
        {
            if (index < 0 || index >= statements.Count)
                throw new VistaDBSQLException(591, index.ToString(), 0, 0);
            return (IQueryStatement)statements[index];
        }

        protected override IQueryResult OnExecuteQuery()
        {
            if (statements.Count == 0)
                return (IQueryResult)null;
            VistaDBDataReader reader = new VistaDBDataReader((IQueryStatement)this, (VistaDBConnection)null, CommandBehavior.Default);
            if (reader.FieldCount > 0)
            {
                VistaDBContext.SQLChannel.Pipe.Send(reader);
            }
            else
            {
                affectedRows = (long)reader.RecordsAffected;
                reader.Dispose();
            }
            return (IQueryResult)null;
        }

        private INextQueryResult CheckBatchExceptions()
        {
            VistaDBException vistaDbException = (VistaDBException)null;
            foreach (Statement statement in (List<Statement>)statements)
            {
                VistaDBException exception = statement.Exception;
                if (exception != null)
                    vistaDbException = vistaDbException != null ? new VistaDBException((Exception)vistaDbException, exception.ErrorId) : exception;
            }
            if (vistaDbException == null)
                return (INextQueryResult)null;
            throw vistaDbException;
        }

        public override INextQueryResult NextResult(VistaDBPipe pipe)
        {
            if (Exception != null)
                throw Exception;
            if (currentStatement >= statements.Count || breakBatch)
                return CheckBatchExceptions();
            Statement statement = statements[currentStatement];
            connection.PrepareCLRContext(pipe);
            IQueryResult resultSet = (IQueryResult)null;
            VistaDBException vistaDbException = (VistaDBException)null;
            try
            {
                resultSet = statement.ExecuteQuery();
            }
            catch (VistaDBException ex)
            {
                vistaDbException = ex;
                DropTemporaryTables();
                statement.Exception = ex;
            }
            finally
            {
                connection.UnprepareCLRContext();
                Connection.LastException = vistaDbException;
            }
            ++currentStatement;
            return (INextQueryResult)new ResultSetData(resultSet, resultSet == null ? (IQuerySchemaInfo)null : statement.GetSchemaInfo(), statement.AffectedRows);
        }

        public override void DoSetParam(string paramName, IParameter parameter)
        {
            prms.Add(paramName, parameter);
        }

        public override void DoSetParam(string paramName, object val, VistaDBType dataType, ParameterDirection direction)
        {
            IParameter parameter = DoGetParam(paramName);
            if (parameter != null)
            {
                parameter.Value = val;
                parameter.DataType = dataType;
                parameter.Direction = direction;
            }
            else
                prms.Add(paramName, (IParameter)new ParamInfo(val, dataType, direction));
        }

        public override IParameter DoGetParam(string paramName)
        {
            IParameter parameter1;
            if (parent == null)
            {
                parameter1 = (IParameter)null;
            }
            else
            {
                IParameter parameter2 = parameter1 = parent.DoGetParam(paramName);
            }
            IParameter parameter3 = parameter1;
            if (parameter3 == null)
            {
                if (!prms.ContainsKey(paramName))
                    return (IParameter)null;
                parameter3 = prms[paramName];
            }
            return parameter3;
        }

        protected bool ReturnParamCascade
        {
            get
            {
                return cascadeReturnParam;
            }
            set
            {
                cascadeReturnParam = value;
            }
        }

        public override IParameter DoGetReturnParameter()
        {
            if (returnParameter == null)
            {
                foreach (IParameter parameter in prms.Values)
                {
                    if (parameter.Direction == ParameterDirection.ReturnValue)
                        return parameter;
                }
                if (cascadeReturnParam && parent is BatchStatement)
                    return parent.DoGetReturnParameter();
            }
            return returnParameter;
        }

        public override void DoSetReturnParameter(IParameter param)
        {
            returnParameter = param;
        }

        public override void DoClearParams()
        {
            prms.Clear();
        }

        public override WhileStatement DoGetCycleStatement()
        {
            if (parent != null)
                return parent.DoGetCycleStatement();
            return (WhileStatement)null;
        }

        public override void DoRegisterTemporaryTableName(string paramName, CreateTableStatement createTableStatement)
        {
            createTableStatement.CreateUniqueName(paramName);
            tempTables.Add(paramName, createTableStatement);
        }

        public override CreateTableStatement DoGetTemporaryTableName(string paramName)
        {
            CreateTableStatement createTableStatement1;
            if (parent == null)
            {
                createTableStatement1 = (CreateTableStatement)null;
            }
            else
            {
                CreateTableStatement createTableStatement2 = createTableStatement1 = parent.DoGetTemporaryTableName(paramName);
            }
            CreateTableStatement createTableStatement3 = createTableStatement1;
            if (createTableStatement3 == null && tempTables.ContainsKey(paramName))
                createTableStatement3 = tempTables[paramName];
            return createTableStatement3;
        }

        public override int SubQueryCount
        {
            get
            {
                return statements.Count;
            }
        }

        public Statement this[int index]
        {
            get
            {
                return statements[index];
            }
        }

        public bool BreakFlag
        {
            set
            {
                breakBatch = value;
            }
        }

        public bool ScopeBreakFlag
        {
            set
            {
                BreakFlag = value;
                if (!value)
                    return;
                for (BatchStatement batch = Batch; batch != null; batch = batch.Batch)
                    batch.BreakFlag = value;
            }
        }

        public void Add(Statement statement)
        {
            hasDDL = hasDDL || statement.HasDDLCommands;
            statements.Add(statement);
        }

        public override void Dispose()
        {
            DropTemporaryTables();
            foreach (Statement statement in (List<Statement>)statements)
                statement.Dispose();
            prms.Clear();
            prms = (Dictionary<string, IParameter>)null;
            statements = (StatementCollection)null;
            tempTables = (Dictionary<string, CreateTableStatement>)null;
            base.Dispose();
        }

        internal void DropTemporaryTables()
        {
            if (tempTables == null)
                return;
            try
            {
                foreach (CreateTableStatement createTableStatement in tempTables.Values)
                {
                    try
                    {
                        Database.DropTable(createTableStatement.TableName);
                        createTableStatement.Dispose();
                    }
                    catch (Exception)
                    {
                    }
                }
            }
            finally
            {
                tempTables.Clear();
            }
        }

        internal class ParamInfo : IParameter
        {
            private ParameterDirection direction = ParameterDirection.Input;
            private VistaDBType dataType = VistaDBType.Unknown;
            private object val;

            internal ParamInfo(object val, VistaDBType dataType, ParameterDirection direction)
            {
                this.val = val;
                this.dataType = dataType;
                this.direction = direction;
            }

            object IParameter.Value
            {
                get
                {
                    return val;
                }
                set
                {
                    val = value;
                }
            }

            VistaDBType IParameter.DataType
            {
                get
                {
                    return dataType;
                }
                set
                {
                    dataType = value;
                }
            }

            ParameterDirection IParameter.Direction
            {
                get
                {
                    return direction;
                }
                set
                {
                    direction = value;
                }
            }
        }

        internal class ResultSetData : INextQueryResult, IDisposable
        {
            private IQueryResult resultSet;
            private IQuerySchemaInfo schema;
            private long affectedRows;

            internal ResultSetData(IQueryResult resultSet, IQuerySchemaInfo schema, long affectedRows)
            {
                this.resultSet = resultSet;
                this.schema = schema;
                this.affectedRows = affectedRows;
            }

            IQueryResult INextQueryResult.ResultSet
            {
                get
                {
                    return resultSet;
                }
            }

            IQuerySchemaInfo INextQueryResult.Schema
            {
                get
                {
                    return schema;
                }
            }

            long INextQueryResult.AffectedRows
            {
                get
                {
                    return affectedRows;
                }
            }

            public void Dispose()
            {
                if (resultSet != null)
                    resultSet.Close();
                resultSet = (IQueryResult)null;
                schema = (IQuerySchemaInfo)null;
            }
        }

        internal class StatementCollection : List<Statement>
        {
            public StatementCollection()
            {
            }

            public StatementCollection(int initial)
              : base(initial)
            {
            }
        }
    }
}
