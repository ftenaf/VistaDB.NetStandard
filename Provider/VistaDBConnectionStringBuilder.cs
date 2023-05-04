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
  [TypeConverter(typeof (VistaDBConnectionStringBuilderConvertor))]
  [DefaultProperty("DataSource")]
  public sealed class VistaDBConnectionStringBuilder : DbConnectionStringBuilder
  {
    private static readonly Dictionary<string, ConnectionKeyword> keywords = new Dictionary<string, ConnectionKeyword>((IEqualityComparer<string>) StringComparer.OrdinalIgnoreCase);
    private static readonly int KEYWORD_COUNT = 12;
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
            keywords = new Dictionary<string, ConnectionKeyword>(KEYWORD_COUNT, (IEqualityComparer<string>) StringComparer.OrdinalIgnoreCase);
            keywords.Add(nameof (Database), ConnectionKeyword.Database);
            keywords.Add("Data Source", ConnectionKeyword.DataSource);
            keywords.Add("Open Mode", ConnectionKeyword.OpenMode);
            keywords.Add(nameof (Password), ConnectionKeyword.Password);
            keywords.Add("Context Connection", ConnectionKeyword.ContextConnection);
            keywords.Add("Min Pool Size", ConnectionKeyword.MinPoolSize);
            keywords.Add("Max Pool Size", ConnectionKeyword.MaxPoolSize);
            keywords.Add("Isolated Storage", ConnectionKeyword.IsolatedStorage);
            keywords.Add("Connect Timeout", ConnectionKeyword.ConnectTimeout);
            keywords.Add(nameof (Pooling), ConnectionKeyword.Pooling);
            keywords.Add("Transaction Mode", ConnectionKeyword.TransactionMode);
            keywords.Add("Encrypt Database", ConnectionKeyword.EncryptDatabase);
            validKeywords = new ArrayList(KEYWORD_COUNT);
            validKeywords.Add((object) nameof (Database));
            validKeywords.Add((object) "Data Source");
            validKeywords.Add((object) "Open Mode");
            validKeywords.Add((object) nameof (Password));
            validKeywords.Add((object) "Context Connection");
            validKeywords.Add((object) "Min Pool Size");
            validKeywords.Add((object) "Max Pool Size");
            validKeywords.Add((object) "Isolated Storage");
            validKeywords.Add((object) "Connect Timeout");
            validKeywords.Add((object) nameof (Pooling));
            validKeywords.Add((object) "Transaction Mode");
            validKeywords.Add((object) "Encrypt Database");
    }

    public VistaDBConnectionStringBuilder()
    {
      Clear();
    }

    public VistaDBConnectionStringBuilder(string connectionString)
    {
      ConnectionString = connectionString;
    }

    [Browsable(false)]
    [DisplayName("Context Connection")]
    public bool ContextConnection
    {
      get
      {
        return contextConnection;
      }
      set
      {
        SetValue("Context Connection", (object) value);
        contextConnection = value;
      }
    }

    [DisplayName("Database")]
    [Browsable(false)]
    public string Database
    {
      get
      {
        return database;
      }
      set
      {
        if (value.Contains(Path.PathSeparator.ToString()) || value.Contains("|"))
        {
          DataSource = value;
        }
        else
        {
          SetValue(nameof (Database), (object) value);
          database = value;
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
        return dataSource;
      }
      set
      {
        SetValue("Data Source", (object) value);
        dataSource = value;
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
        return isolatedStorage;
      }
      set
      {
        SetValue("Isolated Storage", (object) value);
        isolatedStorage = value;
      }
    }

    public override object this[string keyword]
    {
      get
      {
        return GetAt(GetIndex(keyword));
      }
      set
      {
        switch (GetIndex(keyword))
        {
          case ConnectionKeyword.Database:
            Database = ConvertToString(value);
            break;
          case ConnectionKeyword.DataSource:
            DataSource = ConvertToString(value);
            break;
          case ConnectionKeyword.OpenMode:
            OpenMode = ConvertToOpenMode((string) value);
            break;
          case ConnectionKeyword.Password:
            Password = ConvertToString(value);
            break;
          case ConnectionKeyword.ContextConnection:
            ContextConnection = ConvertToBoolean(value);
            break;
          case ConnectionKeyword.MinPoolSize:
            MinPoolSize = ConvertToInt(value);
            break;
          case ConnectionKeyword.MaxPoolSize:
            MaxPoolSize = ConvertToInt(value);
            break;
          case ConnectionKeyword.IsolatedStorage:
            IsolatedStorage = ConvertToBoolean(value);
            break;
          case ConnectionKeyword.ConnectTimeout:
            ConnectTimeout = ConvertToInt(value);
            break;
          case ConnectionKeyword.Pooling:
            Pooling = ConvertToBoolean(value);
            break;
          case ConnectionKeyword.TransactionMode:
            TransactionMode = ConvertToTransactionMode(value);
            break;
          case ConnectionKeyword.END_KEYWORDS:
            throw new ArgumentException("Invalid keyword", (Exception) new VistaDBException(1014, keyword));
          default:
            Remove(keyword);
            break;
        }
      }
    }

    [Browsable(false)]
    public override ICollection Keys
    {
      get
      {
        return (ICollection)validKeywords;
      }
    }

    [DisplayName("Transaction Mode")]
    [RefreshProperties(RefreshProperties.All)]
    [Description("TransactionMode current setting for supporting or ignoring transactions on this connection.")]
    public VistaDBTransaction.TransactionMode TransactionMode
    {
      get
      {
        return transactionMode;
      }
      set
      {
        if (value != VistaDBTransaction.TransactionMode.On)
          SetValue("Transaction Mode", (object) value);
        transactionMode = value;
      }
    }

    [RefreshProperties(RefreshProperties.All)]
    [DisplayName("Pooling")]
    [Description("Whether the connection should be loaded from connection pool or a new one created each time.")]
    public bool Pooling
    {
      get
      {
        return pooling;
      }
      set
      {
        if (value)
          SetValue(nameof (Pooling), (object) value);
        pooling = value;
      }
    }

    [DisplayName("Min Pool Size")]
    [RefreshProperties(RefreshProperties.All)]
    [Description("The minimum number of connections in the connection pool.")]
    public int MinPoolSize
    {
      get
      {
        return minPoolSize;
      }
      set
      {
        if (value < 0)
          throw new ArgumentOutOfRangeException("Min Pool Size", "Invalid value for Min Pool Size");
        if (value < 1)
          value = 1;
        if (value > 100)
          value = 100;
        SetValue("Min Pool Size", (object) value);
        minPoolSize = value;
      }
    }

    [RefreshProperties(RefreshProperties.All)]
    [DisplayName("Max Pool Size")]
    [Description("The maximum number of connections stored in the connection pool")]
    public int MaxPoolSize
    {
      get
      {
        return maxPoolSize;
      }
      set
      {
        if (value < 1 || value > 100)
          throw new ArgumentOutOfRangeException("Max Pool Size", "Invalid Value for Max Pool Size");
        if (value < minPoolSize)
          throw new ArgumentOutOfRangeException("Max Pool Size", "Max Pool Size cannot be less than Min Pool Size");
        SetValue("Max Pool Size", (object) value);
        maxPoolSize = value;
      }
    }

    [RefreshProperties(RefreshProperties.All)]
    [Description("The maximum number of seconds to wait for a valid connection")]
    [DisplayName("Connect Timeout")]
    public int ConnectTimeout
    {
      get
      {
        return connectTimeout;
      }
      set
      {
        SetValue("Connect Timeout", (object) value);
        connectTimeout = value;
      }
    }

    [Description("The connection open mode")]
    [DisplayName("Open Mode")]
    [RefreshProperties(RefreshProperties.All)]
    public VistaDBDatabaseOpenMode OpenMode
    {
      get
      {
        return openMode;
      }
      set
      {
        SetValue("Open Mode", (object) value);
        openMode = value;
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
        return passphrase;
      }
      set
      {
        if (value != null)
        {
          passphrase = value.Trim();
          if (passphrase.Length > 0)
          {
            encryptDatabase = true;
            SetValue(nameof (Password), (object) value);
            passphrase = value;
          }
          else
          {
            passphrase = (string) null;
            SetValue(nameof (Password), (object) null);
          }
        }
        else
        {
          encryptDatabase = false;
          passphrase = (string) null;
          SetValue(nameof (Password), (object) null);
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
        return encryptDatabase;
      }
      set
      {
        SetValue("Encrypt Database", (object) value);
        encryptDatabase = value;
      }
    }

    public override ICollection Values
    {
      get
      {
        ArrayList arrayList = new ArrayList(KEYWORD_COUNT);
        for (int index = 0; index < KEYWORD_COUNT; ++index)
          arrayList[index] = GetAt((ConnectionKeyword) index);
        return (ICollection) arrayList;
      }
    }

    public override void Clear()
    {
      base.Clear();
      for (int index = 0; index < KEYWORD_COUNT; ++index)
        Reset((ConnectionKeyword) index);
    }

    public override bool ContainsKey(string keyword)
    {
      return keywords.ContainsKey(keyword);
    }

    public override bool Remove(string keyword)
    {
            ConnectionKeyword index;
      if (!keywords.TryGetValue(keyword, out index) || !base.Remove((string)validKeywords[(int) index]))
        return false;
      Reset(index);
      return true;
    }

    public override bool ShouldSerialize(string keyword)
    {
            ConnectionKeyword connectionKeyword;
      if (keywords.TryGetValue(keyword, out connectionKeyword))
        return base.ShouldSerialize((string)validKeywords[(int) connectionKeyword]);
      return false;
    }

    public override bool TryGetValue(string keyword, out object value)
    {
            ConnectionKeyword index;
      if (keywords.TryGetValue(keyword, out index))
      {
        value = GetAt(index);
        return true;
      }
      value = (object) null;
      return false;
    }

    private void SetValue(string keyword, object value)
    {
      base[keyword] = value;
    }

    private void Reset(ConnectionKeyword index)
    {
      switch (index)
      {
        case ConnectionKeyword.Database:
          database = string.Empty;
          break;
        case ConnectionKeyword.DataSource:
          dataSource = string.Empty;
          break;
        case ConnectionKeyword.OpenMode:
          openMode = VistaDBDatabaseOpenMode.NonexclusiveReadWrite;
          break;
        case ConnectionKeyword.Password:
          passphrase = string.Empty;
          break;
        case ConnectionKeyword.ContextConnection:
          contextConnection = false;
          break;
        case ConnectionKeyword.MinPoolSize:
          minPoolSize = 1;
          break;
        case ConnectionKeyword.MaxPoolSize:
          maxPoolSize = 100;
          break;
        case ConnectionKeyword.IsolatedStorage:
          isolatedStorage = false;
          break;
        case ConnectionKeyword.ConnectTimeout:
          connectTimeout = 0;
          break;
        case ConnectionKeyword.Pooling:
          Pooling = false;
          break;
        case ConnectionKeyword.TransactionMode:
          transactionMode = VistaDBTransaction.TransactionMode.On;
          break;
        case ConnectionKeyword.EncryptDatabase:
          encryptDatabase = false;
          break;
      }
    }

    private object GetAt(ConnectionKeyword index)
    {
      switch (index)
      {
        case ConnectionKeyword.Database:
          return (object) database;
        case ConnectionKeyword.DataSource:
          return (object) dataSource;
        case ConnectionKeyword.OpenMode:
          return (object) openMode;
        case ConnectionKeyword.Password:
          return (object) passphrase;
        case ConnectionKeyword.ContextConnection:
          return (object) contextConnection;
        case ConnectionKeyword.MinPoolSize:
          return (object) minPoolSize;
        case ConnectionKeyword.MaxPoolSize:
          return (object) maxPoolSize;
        case ConnectionKeyword.IsolatedStorage:
          return (object) isolatedStorage;
        case ConnectionKeyword.ConnectTimeout:
          return (object) connectTimeout;
        case ConnectionKeyword.Pooling:
          return (object) Pooling;
        case ConnectionKeyword.TransactionMode:
          return (object) transactionMode;
        case ConnectionKeyword.EncryptDatabase:
          return (object) encryptDatabase;
        default:
          return (object) null;
      }
    }

    private ConnectionKeyword GetIndex(string keyword)
    {
            ConnectionKeyword connectionKeyword;
      if (keywords.TryGetValue(keyword, out connectionKeyword))
        return connectionKeyword;
      return ConnectionKeyword.END_KEYWORDS;
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
            return (object) ConvertToInstanceDescriptor(options);
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
