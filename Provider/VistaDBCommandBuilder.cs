using System;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using VistaDB.Diagnostic;

namespace VistaDB.Provider
{
  public sealed class VistaDBCommandBuilder : DbCommandBuilder
  {
    internal const string InternalQuoteSuffix = "]";
    internal const string InternalQuotePrefix = "[";

    public VistaDBCommandBuilder()
    {
      this.QuotePrefix = "[";
      this.QuoteSuffix = "]";
    }

    public VistaDBCommandBuilder(VistaDBDataAdapter dataAdapter)
      : this()
    {
      this.DataAdapter = dataAdapter;
    }

    public VistaDBDataAdapter DataAdapter
    {
      get
      {
        return (VistaDBDataAdapter) base.DataAdapter;
      }
      set
      {
        DataAdapter = value;
      }
    }

    [Browsable(false)]
    public override string SchemaSeparator
    {
      get
      {
        return ".";
      }
      set
      {
        if (!value.Equals(".", StringComparison.OrdinalIgnoreCase))
          throw new NotSupportedException();
      }
    }

    [Browsable(false)]
    public override string QuotePrefix
    {
      get
      {
        return "[";
      }
      set
      {
        if (value != "[")
          throw new VistaDBSQLException(1007, "", 0, 0);
        base.QuotePrefix = value;
      }
    }

    [Browsable(false)]
    public override string QuoteSuffix
    {
      get
      {
        return "]";
      }
      set
      {
        if (value != "]")
          throw new VistaDBSQLException(1006, "", 0, 0);
        base.QuoteSuffix = value;
      }
    }

    public VistaDBCommand GetDeleteCommand()
    {
      return (VistaDBCommand) base.GetDeleteCommand();
    }

    public VistaDBCommand GetDeleteCommand(bool useColumnsForParameterNames)
    {
      return (VistaDBCommand) base.GetDeleteCommand(useColumnsForParameterNames);
    }

    public VistaDBCommand GetInsertCommand()
    {
      return (VistaDBCommand) base.GetInsertCommand();
    }

    public VistaDBCommand GetInsertCommand(bool useColumnsForParameterNames)
    {
      return (VistaDBCommand) base.GetInsertCommand(useColumnsForParameterNames);
    }

    public VistaDBCommand GetUpdateCommand()
    {
      return (VistaDBCommand) base.GetUpdateCommand();
    }

    public VistaDBCommand GetUpdateCommand(bool useColumnsForParameterNames)
    {
      return (VistaDBCommand) base.GetUpdateCommand(useColumnsForParameterNames);
    }

    public override string QuoteIdentifier(string unquotedIdentifier)
    {
      return VistaDBCommandBuilder.InternalQuoteIdentifier(unquotedIdentifier);
    }

    public override string UnquoteIdentifier(string quotedIdentifier)
    {
      return VistaDBCommandBuilder.InternalUnquoteIdentifier(quotedIdentifier);
    }

    protected override void ApplyParameterInfo(DbParameter parameter, DataRow row, StatementType statementType, bool whereClause)
    {
      ((VistaDBParameter) parameter).VistaDBType = (VistaDBType) row[SchemaTableColumn.ProviderType];
    }

    protected override string GetParameterName(int parameterOrdinal)
    {
      return "@p" + parameterOrdinal.ToString();
    }

    protected override string GetParameterName(string parameterName)
    {
      return "@" + parameterName;
    }

    protected override string GetParameterPlaceholder(int parameterOrdinal)
    {
      return "@p" + parameterOrdinal.ToString();
    }

    protected override void SetRowUpdatingHandler(DbDataAdapter adapter)
    {
      if (adapter == base.DataAdapter)
        ((VistaDBDataAdapter) adapter).RowUpdating -= new VistaDBRowUpdatingEventHandler(this.VistaDBRowUpdatingHandler);
      else
        ((VistaDBDataAdapter) adapter).RowUpdating += new VistaDBRowUpdatingEventHandler(this.VistaDBRowUpdatingHandler);
    }

    private void VistaDBRowUpdatingHandler(object sender, VistaDBRowUpdatingEventArgs ruevent)
    {
      this.RowUpdatingHandler((RowUpdatingEventArgs) ruevent);
    }

    internal static string InternalQuoteIdentifier(string unquotedIdentifier)
    {
      if (unquotedIdentifier != null)
        return "[" + unquotedIdentifier.Replace("]", "]]") + "]";
      return (string) null;
    }

    internal static string InternalUnquoteIdentifier(string quotedIdentifier)
    {
      if (quotedIdentifier == null)
        return (string) null;
      if (!quotedIdentifier.StartsWith("[", StringComparison.OrdinalIgnoreCase) || !quotedIdentifier.EndsWith("]", StringComparison.OrdinalIgnoreCase))
        return quotedIdentifier;
      return quotedIdentifier.Substring("[".Length, quotedIdentifier.Length - ("[".Length + "]".Length)).Replace("]]", "]");
    }

    internal static Delegate FindBuilder(MulticastDelegate mcd)
    {
      if ((object) mcd != null)
      {
        Delegate[] invocationList = mcd.GetInvocationList();
        for (int index = 0; index < invocationList.Length; ++index)
        {
          if (invocationList[index].Target is VistaDBCommandBuilder)
            return invocationList[index];
        }
      }
      return (Delegate) null;
    }

    public static void DeriveParameters(VistaDBCommand command)
    {
      if (command == null)
        throw new ArgumentNullException(nameof (command));
      command.DeriveParameters();
    }
  }
}
