using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.Design.Serialization;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Reflection;
using VistaDB.Diagnostic;

namespace VistaDB.Provider
{
  [TypeConverter(typeof (VistaDBConnectionStringBuilder.VistaDBConnectionStringBuilderConvertor))]
  [DefaultProperty("DataSource")]
  public sealed class VistaDBConnectionStringBuilder : DbConnectionStringBuilder
  {
    private static readonly Dictionary<string, VistaDBConnectionStringBuilder.ConnectionKeyword> keywords = new Dictionary<string, VistaDBConnectionStringBuilder.ConnectionKeyword>((IEqualityComparer<string>) StringComparer.OrdinalIgnoreCase);
    private static readonly int KEYWORD_COUNT = 12;
    private const string DATABASE_KEYWORD = "Database";
    private const string DATA_SOURCE_KEYWORD = "Data Source";
    private const string OPEN_MODE_KEYWORD = "Open Mode";
    private const string PASSWORD_KEYWORD = "Password";
    private const string CONTEXT_CONN_KEYWORD = "Context Connection";
    private const string POOLING_KEYWORD = "Pooling";
    private const string MIN_POOL_SIZE_KEYWORD = "Min Pool Size";
    private const string MAX_POOL_SIZE_KEYWORD = "Max Pool Size";
    private const string ISOLATED_STORAGE_KEYWORD = "Isolated Storage";
    private const string CONNECT_TIMEOUT_KEYWORD = "Connect Timeout";
    private const string TRANSACTIONMODE_KEYWORD = "Transaction Mode";
    private const string ENCRYPTDATABASE_KEYWORD = "Encrypt Database";
    private const int MIN_POOL_SIZE = 1;
    private const int MAX_POOL_SIZE = 100;
    private const int CONNECT_TIMEOUT_IMMEDIATE = 0;
    private static readonly ArrayList validKeywords;
    private string database;
    private string dataSource;
    private VistaDBDatabaseOpenMode openMode;
    private string passphrase;
    private bool contextConnection;
    private int minPoolSize;
    private int maxPoolSize;
    private bool isolatedStorage;
    private int connectTimeout;
    private bool pooling;
    private VistaDBTransaction.TransactionMode transactionMode;
    private bool encryptDatabase;

    static VistaDBConnectionStringBuilder()
    {
      VistaDBConnectionStringBuilder.keywords = new Dictionary<string, VistaDBConnectionStringBuilder.ConnectionKeyword>(VistaDBConnectionStringBuilder.KEYWORD_COUNT, (IEqualityComparer<string>) StringComparer.OrdinalIgnoreCase);
      VistaDBConnectionStringBuilder.keywords.Add(nameof (Database), VistaDBConnectionStringBuilder.ConnectionKeyword.Database);
      VistaDBConnectionStringBuilder.keywords.Add("Data Source", VistaDBConnectionStringBuilder.ConnectionKeyword.DataSource);
      VistaDBConnectionStringBuilder.keywords.Add("Open Mode", VistaDBConnectionStringBuilder.ConnectionKeyword.OpenMode);
      VistaDBConnectionStringBuilder.keywords.Add(nameof (Password), VistaDBConnectionStringBuilder.ConnectionKeyword.Password);
      VistaDBConnectionStringBuilder.keywords.Add("Context Connection", VistaDBConnectionStringBuilder.ConnectionKeyword.ContextConnection);
      VistaDBConnectionStringBuilder.keywords.Add("Min Pool Size", VistaDBConnectionStringBuilder.ConnectionKeyword.MinPoolSize);
      VistaDBConnectionStringBuilder.keywords.Add("Max Pool Size", VistaDBConnectionStringBuilder.ConnectionKeyword.MaxPoolSize);
      VistaDBConnectionStringBuilder.keywords.Add("Isolated Storage", VistaDBConnectionStringBuilder.ConnectionKeyword.IsolatedStorage);
      VistaDBConnectionStringBuilder.keywords.Add("Connect Timeout", VistaDBConnectionStringBuilder.ConnectionKeyword.ConnectTimeout);
      VistaDBConnectionStringBuilder.keywords.Add(nameof (Pooling), VistaDBConnectionStringBuilder.ConnectionKeyword.Pooling);
      VistaDBConnectionStringBuilder.keywords.Add("Transaction Mode", VistaDBConnectionStringBuilder.ConnectionKeyword.TransactionMode);
      VistaDBConnectionStringBuilder.keywords.Add("Encrypt Database", VistaDBConnectionStringBuilder.ConnectionKeyword.EncryptDatabase);
      VistaDBConnectionStringBuilder.validKeywords = new ArrayList(VistaDBConnectionStringBuilder.KEYWORD_COUNT);
      VistaDBConnectionStringBuilder.validKeywords.Add((object) nameof (Database));
      VistaDBConnectionStringBuilder.validKeywords.Add((object) "Data Source");
      VistaDBConnectionStringBuilder.validKeywords.Add((object) "Open Mode");
      VistaDBConnectionStringBuilder.validKeywords.Add((object) nameof (Password));
      VistaDBConnectionStringBuilder.validKeywords.Add((object) "Context Connection");
      VistaDBConnectionStringBuilder.validKeywords.Add((object) "Min Pool Size");
      VistaDBConnectionStringBuilder.validKeywords.Add((object) "Max Pool Size");
      VistaDBConnectionStringBuilder.validKeywords.Add((object) "Isolated Storage");
      VistaDBConnectionStringBuilder.validKeywords.Add((object) "Connect Timeout");
      VistaDBConnectionStringBuilder.validKeywords.Add((object) nameof (Pooling));
      VistaDBConnectionStringBuilder.validKeywords.Add((object) "Transaction Mode");
      VistaDBConnectionStringBuilder.validKeywords.Add((object) "Encrypt Database");
    }

    public VistaDBConnectionStringBuilder()
    {
      this.Clear();
    }

    public VistaDBConnectionStringBuilder(string connectionString)
    {
      this.ConnectionString = connectionString;
    }

    [Browsable(false)]
    [DisplayName("Context Connection")]
    public bool ContextConnection
    {
      get
      {
        return this.contextConnection;
      }
      set
      {
        this.SetValue("Context Connection", (object) value);
        this.contextConnection = value;
      }
    }

    [DisplayName("Database")]
    [Browsable(false)]
    public string Database
    {
      get
      {
        return this.database;
      }
      set
      {
        if (value.Contains(Path.PathSeparator.ToString()) || value.Contains("|"))
        {
          this.DataSource = value;
        }
        else
        {
          this.SetValue(nameof (Database), (object) value);
          this.database = value;
        }
      }
    }

    [RefreshProperties(RefreshProperties.All)]
    [DisplayName("Data Source")]
    [Description("The name and file location of the database to open.")]
    [Editor("System.Windows.Forms.Design.FileNameEditor, System.Design, Version=2.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a", "System.Drawing.Design.UITypeEditor, System.Drawing, Version=2.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a")]
    public string DataSource
    {
      get
      {
        return this.dataSource;
      }
      set
      {
        this.SetValue("Data Source", (object) value);
        this.dataSource = value;
      }
    }

    [Browsable(false)]
    public override bool IsFixedSize
    {
      get
      {
        return true;
      }
    }

    [Browsable(false)]
    [DisplayName("Isolated Storage")]
    public bool IsolatedStorage
    {
      get
      {
        return this.isolatedStorage;
      }
      set
      {
        this.SetValue("Isolated Storage", (object) value);
        this.isolatedStorage = value;
      }
    }

    public override object this[string keyword]
    {
      get
      {
        return this.GetAt(this.GetIndex(keyword));
      }
      set
      {
        switch (this.GetIndex(keyword))
        {
          case VistaDBConnectionStringBuilder.ConnectionKeyword.Database:
            this.Database = this.ConvertToString(value);
            break;
          case VistaDBConnectionStringBuilder.ConnectionKeyword.DataSource:
            this.DataSource = this.ConvertToString(value);
            break;
          case VistaDBConnectionStringBuilder.ConnectionKeyword.OpenMode:
            this.OpenMode = this.ConvertToOpenMode((string) value);
            break;
          case VistaDBConnectionStringBuilder.ConnectionKeyword.Password:
            this.Password = this.ConvertToString(value);
            break;
          case VistaDBConnectionStringBuilder.ConnectionKeyword.ContextConnection:
            this.ContextConnection = this.ConvertToBoolean(value);
            break;
          case VistaDBConnectionStringBuilder.ConnectionKeyword.MinPoolSize:
            this.MinPoolSize = this.ConvertToInt(value);
            break;
          case VistaDBConnectionStringBuilder.ConnectionKeyword.MaxPoolSize:
            this.MaxPoolSize = this.ConvertToInt(value);
            break;
          case VistaDBConnectionStringBuilder.ConnectionKeyword.IsolatedStorage:
            this.IsolatedStorage = this.ConvertToBoolean(value);
            break;
          case VistaDBConnectionStringBuilder.ConnectionKeyword.ConnectTimeout:
            this.ConnectTimeout = this.ConvertToInt(value);
            break;
          case VistaDBConnectionStringBuilder.ConnectionKeyword.Pooling:
            this.Pooling = this.ConvertToBoolean(value);
            break;
          case VistaDBConnectionStringBuilder.ConnectionKeyword.TransactionMode:
            this.TransactionMode = this.ConvertToTransactionMode(value);
            break;
          case VistaDBConnectionStringBuilder.ConnectionKeyword.END_KEYWORDS:
            throw new ArgumentException("Invalid keyword", (Exception) new VistaDBException(1014, keyword));
          default:
            this.Remove(keyword);
            break;
        }
      }
    }

    [Browsable(false)]
    public override ICollection Keys
    {
      get
      {
        return (ICollection) VistaDBConnectionStringBuilder.validKeywords;
      }
    }

    [DisplayName("Transaction Mode")]
    [RefreshProperties(RefreshProperties.All)]
    [Description("TransactionMode current setting for supporting or ignoring transactions on this connection.")]
    public VistaDBTransaction.TransactionMode TransactionMode
    {
      get
      {
        return this.transactionMode;
      }
      set
      {
        if (value != VistaDBTransaction.TransactionMode.On)
          this.SetValue("Transaction Mode", (object) value);
        this.transactionMode = value;
      }
    }

    [RefreshProperties(RefreshProperties.All)]
    [DisplayName("Pooling")]
    [Description("Whether the connection should be loaded from connection pool or a new one created each time.")]
    public bool Pooling
    {
      get
      {
        return this.pooling;
      }
      set
      {
        if (value)
          this.SetValue(nameof (Pooling), (object) value);
        this.pooling = value;
      }
    }

    [DisplayName("Min Pool Size")]
    [RefreshProperties(RefreshProperties.All)]
    [Description("The minimum number of connections in the connection pool.")]
    public int MinPoolSize
    {
      get
      {
        return this.minPoolSize;
      }
      set
      {
        if (value < 0)
          throw new ArgumentOutOfRangeException("Min Pool Size", "Invalid value for Min Pool Size");
        if (value < 1)
          value = 1;
        if (value > 100)
          value = 100;
        this.SetValue("Min Pool Size", (object) value);
        this.minPoolSize = value;
      }
    }

    [RefreshProperties(RefreshProperties.All)]
    [DisplayName("Max Pool Size")]
    [Description("The maximum number of connections stored in the connection pool")]
    public int MaxPoolSize
    {
      get
      {
        return this.maxPoolSize;
      }
      set
      {
        if (value < 1 || value > 100)
          throw new ArgumentOutOfRangeException("Max Pool Size", "Invalid Value for Max Pool Size");
        if (value < this.minPoolSize)
          throw new ArgumentOutOfRangeException("Max Pool Size", "Max Pool Size cannot be less than Min Pool Size");
        this.SetValue("Max Pool Size", (object) value);
        this.maxPoolSize = value;
      }
    }

    [RefreshProperties(RefreshProperties.All)]
    [Description("The maximum number of seconds to wait for a valid connection")]
    [DisplayName("Connect Timeout")]
    public int ConnectTimeout
    {
      get
      {
        return this.connectTimeout;
      }
      set
      {
        this.SetValue("Connect Timeout", (object) value);
        this.connectTimeout = value;
      }
    }

    [Description("The connection open mode")]
    [DisplayName("Open Mode")]
    [RefreshProperties(RefreshProperties.All)]
    public VistaDBDatabaseOpenMode OpenMode
    {
      get
      {
        return this.openMode;
      }
      set
      {
        this.SetValue("Open Mode", (object) value);
        this.openMode = value;
      }
    }

    [PasswordPropertyText(true)]
    [DisplayName("Password")]
    [Description("The phrase used in the encryption system to build up the encryption key.")]
    [RefreshProperties(RefreshProperties.All)]
    public string Password
    {
      get
      {
        return this.passphrase;
      }
      set
      {
        if (value != null)
        {
          this.passphrase = value.Trim();
          if (this.passphrase.Length > 0)
          {
            this.encryptDatabase = true;
            this.SetValue(nameof (Password), (object) value);
            this.passphrase = value;
          }
          else
          {
            this.passphrase = (string) null;
            this.SetValue(nameof (Password), (object) null);
          }
        }
        else
        {
          this.encryptDatabase = false;
          this.passphrase = (string) null;
          this.SetValue(nameof (Password), (object) null);
        }
      }
    }

    [RefreshProperties(RefreshProperties.All)]
    [Description("Encryption status for the entire database")]
    [DisplayName("Encrypt Database")]
    public bool EncryptDatabase
    {
      get
      {
        return this.encryptDatabase;
      }
      set
      {
        this.SetValue("Encrypt Database", (object) value);
        this.encryptDatabase = value;
      }
    }

    public override ICollection Values
    {
      get
      {
        ArrayList arrayList = new ArrayList(VistaDBConnectionStringBuilder.KEYWORD_COUNT);
        for (int index = 0; index < VistaDBConnectionStringBuilder.KEYWORD_COUNT; ++index)
          arrayList[index] = this.GetAt((VistaDBConnectionStringBuilder.ConnectionKeyword) index);
        return (ICollection) arrayList;
      }
    }

    public override void Clear()
    {
      base.Clear();
      for (int index = 0; index < VistaDBConnectionStringBuilder.KEYWORD_COUNT; ++index)
        this.Reset((VistaDBConnectionStringBuilder.ConnectionKeyword) index);
    }

    public override bool ContainsKey(string keyword)
    {
      return VistaDBConnectionStringBuilder.keywords.ContainsKey(keyword);
    }

    public override bool Remove(string keyword)
    {
      VistaDBConnectionStringBuilder.ConnectionKeyword index;
      if (!VistaDBConnectionStringBuilder.keywords.TryGetValue(keyword, out index) || !base.Remove((string) VistaDBConnectionStringBuilder.validKeywords[(int) index]))
        return false;
      this.Reset(index);
      return true;
    }

    public override bool ShouldSerialize(string keyword)
    {
      VistaDBConnectionStringBuilder.ConnectionKeyword connectionKeyword;
      if (VistaDBConnectionStringBuilder.keywords.TryGetValue(keyword, out connectionKeyword))
        return base.ShouldSerialize((string) VistaDBConnectionStringBuilder.validKeywords[(int) connectionKeyword]);
      return false;
    }

    public override bool TryGetValue(string keyword, out object value)
    {
      VistaDBConnectionStringBuilder.ConnectionKeyword index;
      if (VistaDBConnectionStringBuilder.keywords.TryGetValue(keyword, out index))
      {
        value = this.GetAt(index);
        return true;
      }
      value = (object) null;
      return false;
    }

    private void SetValue(string keyword, object value)
    {
      base[keyword] = value;
    }

    private void Reset(VistaDBConnectionStringBuilder.ConnectionKeyword index)
    {
      switch (index)
      {
        case VistaDBConnectionStringBuilder.ConnectionKeyword.Database:
          this.database = string.Empty;
          break;
        case VistaDBConnectionStringBuilder.ConnectionKeyword.DataSource:
          this.dataSource = string.Empty;
          break;
        case VistaDBConnectionStringBuilder.ConnectionKeyword.OpenMode:
          this.openMode = VistaDBDatabaseOpenMode.NonexclusiveReadWrite;
          break;
        case VistaDBConnectionStringBuilder.ConnectionKeyword.Password:
          this.passphrase = string.Empty;
          break;
        case VistaDBConnectionStringBuilder.ConnectionKeyword.ContextConnection:
          this.contextConnection = false;
          break;
        case VistaDBConnectionStringBuilder.ConnectionKeyword.MinPoolSize:
          this.minPoolSize = 1;
          break;
        case VistaDBConnectionStringBuilder.ConnectionKeyword.MaxPoolSize:
          this.maxPoolSize = 100;
          break;
        case VistaDBConnectionStringBuilder.ConnectionKeyword.IsolatedStorage:
          this.isolatedStorage = false;
          break;
        case VistaDBConnectionStringBuilder.ConnectionKeyword.ConnectTimeout:
          this.connectTimeout = 0;
          break;
        case VistaDBConnectionStringBuilder.ConnectionKeyword.Pooling:
          this.Pooling = false;
          break;
        case VistaDBConnectionStringBuilder.ConnectionKeyword.TransactionMode:
          this.transactionMode = VistaDBTransaction.TransactionMode.On;
          break;
        case VistaDBConnectionStringBuilder.ConnectionKeyword.EncryptDatabase:
          this.encryptDatabase = false;
          break;
      }
    }

    private object GetAt(VistaDBConnectionStringBuilder.ConnectionKeyword index)
    {
      switch (index)
      {
        case VistaDBConnectionStringBuilder.ConnectionKeyword.Database:
          return (object) this.database;
        case VistaDBConnectionStringBuilder.ConnectionKeyword.DataSource:
          return (object) this.dataSource;
        case VistaDBConnectionStringBuilder.ConnectionKeyword.OpenMode:
          return (object) this.openMode;
        case VistaDBConnectionStringBuilder.ConnectionKeyword.Password:
          return (object) this.passphrase;
        case VistaDBConnectionStringBuilder.ConnectionKeyword.ContextConnection:
          return (object) this.contextConnection;
        case VistaDBConnectionStringBuilder.ConnectionKeyword.MinPoolSize:
          return (object) this.minPoolSize;
        case VistaDBConnectionStringBuilder.ConnectionKeyword.MaxPoolSize:
          return (object) this.maxPoolSize;
        case VistaDBConnectionStringBuilder.ConnectionKeyword.IsolatedStorage:
          return (object) this.isolatedStorage;
        case VistaDBConnectionStringBuilder.ConnectionKeyword.ConnectTimeout:
          return (object) this.connectTimeout;
        case VistaDBConnectionStringBuilder.ConnectionKeyword.Pooling:
          return (object) this.Pooling;
        case VistaDBConnectionStringBuilder.ConnectionKeyword.TransactionMode:
          return (object) this.transactionMode;
        case VistaDBConnectionStringBuilder.ConnectionKeyword.EncryptDatabase:
          return (object) this.encryptDatabase;
        default:
          return (object) null;
      }
    }

    private VistaDBConnectionStringBuilder.ConnectionKeyword GetIndex(string keyword)
    {
      VistaDBConnectionStringBuilder.ConnectionKeyword connectionKeyword;
      if (VistaDBConnectionStringBuilder.keywords.TryGetValue(keyword, out connectionKeyword))
        return connectionKeyword;
      return VistaDBConnectionStringBuilder.ConnectionKeyword.END_KEYWORDS;
    }

    private int ConvertToInt(object value)
    {
      return ((IConvertible) value).ToInt32((IFormatProvider) CultureInfo.InvariantCulture);
    }

    private string ConvertToString(object value)
    {
      return ((IConvertible) value)?.ToString();
    }

    private bool ConvertToBoolean(object value)
    {
      if (value is string)
      {
        string upperInvariant = ((string) value).ToUpperInvariant();
        if (upperInvariant == "TRUE")
          return true;
        if (upperInvariant == "FALSE")
          return false;
      }
      return ((IConvertible) value).ToBoolean((IFormatProvider) CultureInfo.InvariantCulture);
    }

    private VistaDBDatabaseOpenMode ConvertToOpenMode(string s)
    {
      if (string.Compare(s, "EXCLUSIVEREADWRITE", StringComparison.OrdinalIgnoreCase) == 0)
        return VistaDBDatabaseOpenMode.ExclusiveReadWrite;
      if (string.Compare(s, "EXCLUSIVEREADONLY", StringComparison.OrdinalIgnoreCase) == 0)
        return VistaDBDatabaseOpenMode.ExclusiveReadOnly;
      if (string.Compare(s, "NONEXCLUSIVEREADWRITE", StringComparison.OrdinalIgnoreCase) == 0)
        return VistaDBDatabaseOpenMode.NonexclusiveReadWrite;
      if (string.Compare(s, "NONEXCLUSIVEREADONLY", StringComparison.OrdinalIgnoreCase) == 0)
        return VistaDBDatabaseOpenMode.NonexclusiveReadOnly;
      if (string.Compare(s, "SHAREDREADONLY", StringComparison.OrdinalIgnoreCase) == 0)
        return VistaDBDatabaseOpenMode.SharedReadOnly;
      throw new FormatException("Invalid open mode: " + s);
    }

    private VistaDBTransaction.TransactionMode ConvertToTransactionMode(object value)
    {
      string strA = value.ToString();
      if (string.Compare(strA, "on", StringComparison.OrdinalIgnoreCase) == 0)
        return VistaDBTransaction.TransactionMode.On;
      if (string.Compare(strA, "off", StringComparison.OrdinalIgnoreCase) == 0)
        return VistaDBTransaction.TransactionMode.Off;
      if (string.Compare(strA, "ignore", StringComparison.OrdinalIgnoreCase) == 0)
        return VistaDBTransaction.TransactionMode.Ignore;
      throw new FormatException("TransactionMode mode must be one of the following: ON | OFF | IGNORE");
    }

    internal sealed class VistaDBConnectionStringBuilderConvertor : ExpandableObjectConverter
    {
      public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
      {
        if (destinationType != typeof (InstanceDescriptor))
          return base.CanConvertTo(context, destinationType);
        return true;
      }

      public override object ConvertTo(ITypeDescriptorContext context, CultureInfo culture, object value, Type destinationType)
      {
        if (destinationType == null)
          throw new ArgumentNullException(nameof (destinationType));
        if (destinationType == typeof (InstanceDescriptor))
        {
          VistaDBConnectionStringBuilder options = value as VistaDBConnectionStringBuilder;
          if (options != null)
            return (object) this.ConvertToInstanceDescriptor(options);
        }
        return base.ConvertTo(context, culture, value, destinationType);
      }

      private InstanceDescriptor ConvertToInstanceDescriptor(VistaDBConnectionStringBuilder options)
      {
        return new InstanceDescriptor((MemberInfo) typeof (VistaDBConnectionStringBuilder).GetConstructor(new Type[1]
        {
          typeof (string)
        }), (ICollection) new object[1]
        {
          (object) options.ConnectionString
        });
      }
    }

    private enum ConnectionKeyword
    {
      Database,
      DataSource,
      OpenMode,
      Password,
      ContextConnection,
      MinPoolSize,
      MaxPoolSize,
      IsolatedStorage,
      ConnectTimeout,
      Pooling,
      TransactionMode,
      EncryptDatabase,
      END_KEYWORDS,
    }
  }
}
