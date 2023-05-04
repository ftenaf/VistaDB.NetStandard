using System;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL;

namespace VistaDB.Provider
{
	public sealed class VistaDBCommand : DbCommand, ICloneable, IDisposable
	{
		private static string CmdMarker = '@'.ToString();
		private static string QuestionMark = '?'.ToString();
        private string commandText;
		private CommandType commandType;
		private UpdateRowSource updateRowSource;
		private VistaDBConnection connection;
		private IQueryStatement queryStatements;
		private VistaDBParameter returnParameter;
		private int commandTimeout;
		private bool designTimeVisible;

		private VistaDBCommand(VistaDBCommand cmd)
		{
			commandText = cmd.commandText;
			commandType = cmd.commandType;
			updateRowSource = cmd.updateRowSource;
			connection = cmd.connection;
			commandTimeout = cmd.commandTimeout;
			Parameters = new VistaDBParameterCollection();
			queryStatements = null;
			foreach (ICloneable parameter in (DbParameterCollection)cmd.Parameters)
			{
				Parameters.Add(parameter.Clone());
			}
		}

		public VistaDBCommand()
		{
			commandText = string.Empty;
			commandType = CommandType.Text;
			updateRowSource = UpdateRowSource.None;
			connection = null;
			Parameters = new VistaDBParameterCollection();
			queryStatements = null;
			commandTimeout = 30;
		}

		public VistaDBCommand(string commandText)
		  : this()
		{
			this.commandText = commandText;
		}

		public VistaDBCommand(string commandText, VistaDBConnection connection)
		  : this(commandText)
		{
			this.connection = connection;
		}

		public override string CommandText
		{
			get
			{
				return commandText;
			}
			set
			{
				lock (SyncRoot)
				{
					if (commandText != null && value != null && string.Compare(commandText, value, false, CultureInfo.InvariantCulture) == 0)
						return;
					FreeQuery();
					commandText = value;
				}
			}
		}

		public override int CommandTimeout
		{
			get
			{
				return commandTimeout;
			}
			set
			{
				if (value < 0 || value > 600)
					throw new ArgumentOutOfRangeException(nameof(CommandTimeout));
				commandTimeout = value;
			}
		}

		public void ResetCommandTimeout()
		{
			commandTimeout = 30;
		}

		public override CommandType CommandType
		{
			get
			{
				return commandType;
			}
			set
			{
				if (value != CommandType.Text && value != CommandType.StoredProcedure)
					throw new VistaDBException(1001);
				commandType = value;
			}
		}

		public new VistaDBConnection Connection
		{
			get
			{
				return connection;
			}
			set
			{
				lock (SyncRoot)
				{
					if (connection == value)
						return;
					FreeQuery();
					connection = value;
				}
			}
		}

		[DesignOnly(true)]
		[Browsable(false)]
		public override bool DesignTimeVisible
		{
			get
			{
				return designTimeVisible;
			}
			set
			{
				designTimeVisible = value;
				TypeDescriptor.Refresh(this);
			}
		}

		public bool HasDDLCommands
		{
			get
			{
				lock (SyncRoot)
				{
					CreateQuery();
					return queryStatements.HasDDLCommands;
				}
			}
		}

		public new VistaDBParameterCollection Parameters { get; private set; }

		public new VistaDBTransaction Transaction
		{
			get
			{
				if (connection != null)
					return connection.Transaction;
				return null;
			}
			set
			{
				if (value == null)
					return;
				Connection = value.Connection;
			}
		}

		public override UpdateRowSource UpdatedRowSource
		{
			get
			{
				return updateRowSource;
			}
			set
			{
				updateRowSource = value;
			}
		}

		protected override DbConnection DbConnection
		{
			get
			{
				return Connection;
			}
			set
			{
				Connection = (VistaDBConnection)value;
			}
		}

		protected override DbParameterCollection DbParameterCollection
		{
			get
			{
				return Parameters;
			}
		}

		protected override DbTransaction DbTransaction
		{
			get
			{
				return Transaction;
			}
			set
			{
				Transaction = (VistaDBTransaction)value;
			}
		}

		[Obsolete("This method is not supported in 4.x and should not be used")]
		public VistaDBExecutionPlan BuildEstimatedExecutionPlan()
		{
			return null;
		}

		public override void Cancel()
		{
		}

		public new VistaDBParameter CreateParameter()
		{
			return new VistaDBParameter();
		}

		public new VistaDBDataReader ExecuteReader()
		{
			return ExecuteReader(CommandBehavior.Default);
		}

		public new VistaDBDataReader ExecuteReader(CommandBehavior behavior)
		{
			lock (SyncRoot)
			{
				PrepareQuery();
				VistaDBDataReader vistaDbDataReader = new VistaDBDataReader(queryStatements, Connection, behavior);
				connection.RetrieveConnectionInfo();
				return vistaDbDataReader;
			}
		}

		public override int ExecuteNonQuery()
		{
			ExecQuery(out long affectedRows);
			return (int)affectedRows;
		}

		public override object ExecuteScalar()
		{
			long affectedRows;
			if (CommandType != CommandType.StoredProcedure)
				return ExecQuery(out affectedRows);
			returnParameter = new VistaDBParameter
			{
				Direction = ParameterDirection.ReturnValue,
				VistaDBType = VistaDBType.Unknown
			};
			object obj = null;
			try
			{
				obj = ExecQuery(out affectedRows);
				if (obj == null)
					return returnParameter.Value;
			}
			finally
			{
				returnParameter = null;
			}
			return obj;
		}

		public override void Prepare()
		{
			PrepareQuery();
		}

		protected override DbParameter CreateDbParameter()
		{
			return CreateParameter();
		}

		protected override void Dispose(bool disposing)
		{
			if (disposing)
			{
				CacheFactory.Reset();
				if (queryStatements != null && !queryStatements.Disposed && connection != null)
					FreeQuery();
			}
			queryStatements = null;
			connection = null;
			returnParameter = null;
			Parameters = null;
			base.Dispose(disposing);
		}

		protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
		{
			return ExecuteReader(behavior);
		}

		internal void DeriveParameters()
		{
			if (CommandType != CommandType.StoredProcedure)
				throw new NotSupportedException(CommandType.ToString());
			if (queryStatements == null)
				PrepareQuery();
			using (VistaDBCommand vistaDbCommand = new VistaDBCommand())
			{
				vistaDbCommand.Connection = connection;
				vistaDbCommand.CommandText = string.Format("SELECT * FROM sp_stored_procedures() WHERE PROC_NAME = '{0}' AND PARAM_ORDER >= 0", SQLParser.GetTableName(CommandText, TokenType.String, 0, 0));
				Parameters.Clear();
				IDatabase database = null;
				using (VistaDBDataReader vistaDbDataReader = vistaDbCommand.ExecuteReader())
				{
					while (vistaDbDataReader.Read())
					{
						string parameterName = vistaDbDataReader["PARAM_NAME"] as string;
						VistaDBType int32 = (VistaDBType)vistaDbDataReader.GetInt32(5);
						bool flag = (bool)vistaDbDataReader["IS_PARAM_OUT"];
						if (vistaDbDataReader["DEFAULT_VALUE"] is string str)
						{
							VistaDBParameter parameter = new VistaDBParameter(parameterName, int32)
							{
								Direction = flag ? ParameterDirection.InputOutput : ParameterDirection.Input
							};
							if (str.Equals("NULL", StringComparison.OrdinalIgnoreCase))
							{
								parameter.Value = DBNull.Value;
							}
							else
							{
								if (database == null)
									database = ((Statement)queryStatements.SubQuery(0)).Database;
								if (!string.IsNullOrEmpty(str))
								{
									IColumn emtpyUnicodeColumn = database.CreateEmtpyUnicodeColumn();
									emtpyUnicodeColumn.Value = str.Trim('\'');
									IColumn emptyColumn = database.CreateEmptyColumn(int32);
									database.Conversion.Convert(emtpyUnicodeColumn, emptyColumn);
									parameter.Value = emptyColumn.Value;
								}
							}
							Parameters.Add(parameter);
						}
					}
				}
			}
		}

		private object ExecQuery(out long affectedRows)
		{
			object obj = null;
			affectedRows = 0L;
			lock (SyncRoot)
			{
				PrepareQuery();
				if (returnParameter != null)
					queryStatements.DoSetReturnParameter(returnParameter);
				VistaDBDataReader vistaDbDataReader = VistaDBDataReader.NonQueryReader(queryStatements);
				try
				{
					obj = vistaDbDataReader.ExecQuery(ref affectedRows);
				}
				finally
				{
					vistaDbDataReader.Close();
					connection.FreeQuery(queryStatements, true);
				}
				connection.RetrieveConnectionInfo();
			}
			return obj;
		}

		private IQueryStatement ThisQuery
		{
			get
			{
				if (queryStatements != null && queryStatements.Disposed)
					queryStatements = null;
				return queryStatements;
			}
		}

		private void PrepareQuery()
		{
			CreateQuery();
			queryStatements.DoClearParams();
			foreach (VistaDBParameter parameter in (DbParameterCollection)Parameters)
			{
				parameter.Prepare();
				if (parameter.Direction == ParameterDirection.Input)
					queryStatements.DoSetParam(parameter.NativeParameterName, parameter.Value == DBNull.Value ? null : parameter.Value, parameter.VistaDBType, parameter.Direction);
				else
					queryStatements.DoSetParam(parameter.NativeParameterName, parameter);
			}
		}

		private void CleanupQuestionParams()
		{
			if (commandType != CommandType.Text)
				return;
			int num = 0;
			string commandText = this.commandText;
			Regex regex1 = new Regex("([=><]\\s*)\\?");
			Regex regex2 = new Regex("'(([^']|'')*)'");
			Match match1 = regex1.Match(commandText);
			Match match2 = regex2.Match(commandText);
			StringBuilder stringBuilder = new StringBuilder();
			int startIndex = 0;
			while (match1.Success)
			{
				while (match2.Success && match2.Index + match2.Length < match1.Index)
					match2 = match2.NextMatch();
				if (match2.Success && match1.Index >= match2.Index && match1.Index + match1.Length <= match2.Index + match2.Length)
				{
					match1 = match1.NextMatch();
				}
				else
				{
					stringBuilder.Append(commandText.Substring(startIndex, match1.Index - startIndex));
					stringBuilder.Append(match1.Groups[1].Value);
					VistaDBParameter parameter;
					if (num < Parameters.Count)
					{
						parameter = Parameters[num++];
					}
					else
					{
						parameter = new VistaDBParameter("p" + (++num).ToString(), null);
						Parameters.Add(parameter);
					}
					stringBuilder.Append(CmdMarker);
					stringBuilder.Append(parameter.NativeParameterName);
					startIndex = match1.Index + match1.Length;
					match1 = match1.NextMatch();
				}
			}
			stringBuilder.Append(commandText.Substring(startIndex));
			this.commandText = stringBuilder.ToString();
		}

		private void CreateQuery()
		{
			if (connection == null)
				throw new VistaDBSQLException(1011, string.Empty, 0, 0);
			if (ThisQuery == null)
			{
				if (commandText == null || commandText.Length == 0)
					throw new VistaDBSQLException(1008, string.Empty, 0, 0);
				if (commandText.Contains(QuestionMark))
					CleanupQuestionParams();
				queryStatements = connection.CreateQuery(commandText);
				if (queryStatements == null)
					throw new VistaDBSQLException(1008, string.Empty, 0, 0);
			}
			else
				ThisQuery.ResetResult();
		}

		private void FreeQuery()
		{
			if (connection != null && queryStatements != null)
				connection.FreeQuery(queryStatements, true);
			queryStatements = null;
		}

		private object SyncRoot
		{
			get
			{
				if (connection == null)
					return this;
				return connection;
			}
		}

		object ICloneable.Clone()
		{
			lock (SyncRoot)
				return (object)new VistaDBCommand(this);
		}
	}
}
