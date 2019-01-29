using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal class SQLParser
  {
    public static string CHAR_TYPE = "CHAR";
    public static string CHARACTER_TYPE = "CHARACTER";
    public static string NCHAR_TYPE = "NCHAR";
    public static string VARCHAR_TYPE = "VARCHAR";
    public static string NVARCHAR_TYPE = "NVARCHAR";
    public static string TEXT_TYPE = "TEXT";
    public static string NTEXT_TYPE = "NTEXT";
    public static string TINYINT_TYPE = "TINYINT";
    public static string SMALLINT_TYPE = "SMALLINT";
    public static string INT_TYPE = "INT";
    public static string INTEGER_TYPE = "INTEGER";
    public static string BIGINT_TYPE = "BIGINT";
    public static string REAL_TYPE = "REAL";
    public static string FLOAT_TYPE = "FLOAT";
    public static string DEC_TYPE = "DECIMAL";
    public static string DECIMAL_TYPE = "DEC";
    public static string NUMERIC_TYPE = "NUMERIC";
    public static string MONEY_TYPE = "MONEY";
    public static string SMALLMONEY_TYPE = "SMALLMONEY";
    public static string BIT_TYPE = "BIT";
    public static string SMALLDATETIME_TYPE = "SMALLDATETIME";
    public static string DATETIME_TYPE = "DATETIME";
    public static string IMAGE_TYPE = "IMAGE";
    public static string VARBINARY_TYPE = "VARBINARY";
    public static string BINARY_TYPE = "BINARY";
    public static string UNIQUEIDENTIFIER_TYPE = "UNIQUEIDENTIFIER";
    public static string TIMESTAMP_TYPE = "TIMESTAMP";
    public static string ROWVERSION_TYPE = "ROWVERSION";
    public static string VARYING_TYPE_PART = "VARYING";
    public static string NATIONAL_TYPE_PART = "NATIONAL";
    public static string PRECISION_TYPE_PART = "PRECISION";
    private static int[] typesWithLen = new int[27]{ int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue, -1, -1, -1, -1, -1, -1, -1, -1, int.MaxValue, int.MaxValue, -1, -1, -1, -1, -1, -1, int.MaxValue, int.MaxValue, -1, -1, -1, -1, -1 };
    private static int[] typesWithScale = new int[26]{ -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, int.MaxValue, int.MaxValue, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -2 };
    private static VistaDBType[] typesWithMaxLen = new VistaDBType[27]{ VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Text, VistaDBType.NText, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Image, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown };
    private static VistaDBType[] sqlToNativeDataType = new VistaDBType[27]{ VistaDBType.Char, VistaDBType.NChar, VistaDBType.VarChar, VistaDBType.NVarChar, VistaDBType.Text, VistaDBType.NText, VistaDBType.TinyInt, VistaDBType.SmallInt, VistaDBType.Int, VistaDBType.BigInt, VistaDBType.Real, VistaDBType.Float, VistaDBType.Decimal, VistaDBType.Decimal, VistaDBType.Money, VistaDBType.SmallMoney, VistaDBType.Bit, VistaDBType.SmallDateTime, VistaDBType.DateTime, VistaDBType.Image, VistaDBType.VarBinary, VistaDBType.VarBinary, VistaDBType.UniqueIdentifier, VistaDBType.Unknown, VistaDBType.Timestamp, VistaDBType.Float, VistaDBType.Unknown };
    private static Dictionary<string, int> reservedWords = new Dictionary<string, int>((IEqualityComparer<string>) StringComparer.OrdinalIgnoreCase);
    private static Dictionary<string, SqlDataType> typeNames = new Dictionary<string, SqlDataType>((IEqualityComparer<string>) StringComparer.OrdinalIgnoreCase);
    public const string UNARY_SUFFIX = " UNARY";
    public const string COMMA_KEYWORD = ",";
    public const string LEFT_BRACKET_KEYWORD = "(";
    public const string RIGHT_BRACKET_KEYWORD = ")";
    public const string LEFT_SQ_BRACKET_KEYWORD = "[";
    public const string RIGHT_SQ_BRACKET_KEYWORD = "]";
    public const string AS_KEYWORD = "AS";
    public const string ALL_KEYWORD = "ALL";
    public const string DISTINCT_KEYWORD = "DISTINCT";
    public const string DISTINCTROW_KEYWORD = "DISTINCTROW";
    public const string TOP_KEYWORD = "TOP";
    public const string FROM_KEYWORD = "FROM";
    public const string WHERE_KEYWORD = "WHERE";
    public const string NULL_KEYWORD = "NULL";
    public const string TRUE_KEYWORD = "TRUE";
    public const string FALSE_KEYWORD = "FALSE";
    public const string YES_KEYWORD = "YES";
    public const string NO_KEYWORD = "NO";
    public const string INNER_KEYWORD = "INNER";
    public const string LEFT_KEYWORD = "LEFT";
    public const string RIGHT_KEYWORD = "RIGHT";
    public const string OUT_KEYWORD = "OUT";
    public const string OUTPUT_KEYWORD = "OUTPUT";
    public const string OUTER_KEYWORD = "OUTER";
    public const string CROSS_KEYWORD = "CROSS";
    public const string JOIN_KEYWORD = "JOIN";
    public const string HAVING_KEYWORD = "HAVING";
    public const string GROUP_KEYWORD = "GROUP";
    public const string ORDER_KEYWORD = "ORDER";
    public const string UNION_KEYWORD = "UNION";
    public const string ON_KEYWORD = "ON";
    public const string BY_KEYWORD = "BY";
    public const string ASC_KEYWORD = "ASC";
    public const string DESC_KEYWORD = "DESC";
    public const string ASTERIX_KEYWORD = "*";
    public const string SELECT_KEYWORD = "SELECT";
    public const string INSERT_KEYWORD = "INSERT";
    public const string UPDATE_KEYWORD = "UPDATE";
    public const string DELETE_KEYWORD = "DELETE";
    public const string ESCAPE_KEYWORD = "ESCAPE";
    public const string DOT_KEYWORD = ".";
    public const string INTO_KEYWORD = "INTO";
    public const string VALUES_KEYWORD = "VALUES";
    public const string DEFAULT_KEYWORD = "DEFAULT";
    public const string SEMICOLON_KEYWORD = ";";
    public const string SET_KEYWORD = "SET";
    public const string CREATE_KEYWORD = "CREATE";
    public const string DATABASE_KEYWORD = "DATABASE";
    public const string PASSWORD_KEYWORD = "PASSWORD";
    public const string PAGE_KEYWORD = "PAGE";
    public const string SIZE_KEYWORD = "SIZE";
    public const string LCID_KEYWORD = "LCID";
    public const string CASE_KEYWORD = "CASE";
    public const string SENSITIVE_KEYWORD = "SENSITIVE";
    public const string TABLE_KEYWORD = "TABLE";
    public const string DESCRIPTION_KEYWORD = "DESCRIPTION";
    public const string ENCRYPTED_KEYWORD = "ENCRYPTED";
    public const string PACKED_KEYWORD = "PACKED";
    public const string IDENTITY_KEYWORD = "IDENTITY";
    public const string CAPTION_KEYWORD = "CAPTION";
    public const string CODE_KEYWORD = "CODE";
    public const string CONSTRAINT_KEYWORD = "CONSTRAINT";
    public const string PRIMARY_KEYWORD = "PRIMARY";
    public const string KEY_KEYWORD = "KEY";
    public const string UNIQUE_KEYWORD = "UNIQUE";
    public const string CLUSTERED_KEYWORD = "CLUSTERED";
    public const string NONCLUSTERED_KEYWORD = "NONCLUSTERED";
    public const string FOREIGN_KEYWORD = "FOREIGN";
    public const string REFERENCES_KEYWORD = "REFERENCES";
    public const string ACTION_KEYWORD = "ACTION";
    public const string RETURN_KEYWORD = "RETURN";
    public const string WHILE_KEYWORD = "WHILE";
    public const string CONTINUE_KEYWORD = "CONTINUE";
    public const string BREAK_KEYWORD = "BREAK";
    public const string RETURNS_KEYWORD = "RETURNS";
    public const string CASCADE_KEYWORD = "CASCADE";
    public const string FULLTEXT_KEYWORD = "FULLTEXT";
    public const string ROWGUIDCOL_KEYWORD = "ROWGUIDCOL";
    public const string READ_KEYWORD = "READ";
    public const string ONLY_KEYWORD = "ONLY";
    public const string INDEX_KEYWORD = "INDEX";
    public const string DROP_KEYWORD = "DROP";
    public const string TRUNCATE_KEYWORD = "TRUNCATE";
    public const string ALTER_KEYWORD = "ALTER";
    public const string COLUMN_KEYWORD = "COLUMN";
    public const string ADD_KEYWORD = "ADD";
    public const string AT_SYMBOL_KEYWORD = "@";
    public const string SOME_KEYWORD = "SOME";
    public const string ANY_KEYWORD = "ANY";
    public const string OPTIMIZATION_KEYWORD = "OPTIMIZATION";
    public const string SYNCHRONIZATION_KEYWORD = "SYNCHRONIZATION";
    public const string OFF_KEYWORD = "OFF";
    public const string VIEW_KEYWORD = "VIEW";
    public const string TRIGGER_KEYWORD = "TRIGGER";
    public const string FOR_KEYWORD = "FOR";
    public const string INSTEAD_KEYWORD = "INSTEAD";
    public const string OF_KEYWORD = "OF";
    public const string AFTER_KEYWORD = "AFTER";
    public const string BEGIN_KEYWORD = "BEGIN";
    public const string COMMIT_KEYWORD = "COMMIT";
    public const string ROLLBACK_KEYWORD = "ROLLBACK";
    public const string TRANSACTION_KEYWORD = "TRANSACTION";
    public const string TRY_KEYWORD = "TRY";
    public const string CATCH_KEYWORD = "CATCH";
    public const string TRANS_KEYWORD = "TRANS";
    public const string ASSEMBLY_KEYWORD = "ASSEMBLY";
    public const string PROCEDURE_KEYWORD = "PROCEDURE";
    public const string FUNCTION_KEYWORD = "FUNCTION";
    public const string PROC_KEYWORD = "PROC";
    public const string EXTERNAL_KEYWORD = "EXTERNAL";
    public const string NAME_KEYWORD = "NAME";
    public const string EXECUTE_KEYWORD = "EXECUTE";
    public const string EXEC_KEYWORD = "EXEC";
    public const string DECLARE_KEYWORD = "DECLARE";
    public const string CHECK_KEYWORD = "CHECK";
    public const string REBUILD_KEYWORD = "REBUILD";
    public const string ISOLATED_KEYWORD = "ISOLATED";
    public const string STORAGE_KEYWORD = "STORAGE";
    public const string INMEMORY_KEYWORD = "INMEMORY";
    public const string DATA_KEYWORD = "DATA";
    public const string SOURCE_KEYWORD = "SOURCE";
    public const string OPEN_KEYWORD = "OPEN";
    public const string MODE_KEYWORD = "MODE";
    public const string EXCLUSIVEREADWRITE_KEYWORD = "EXCLUSIVEREADWRITE";
    public const string EXCLUSIVEREADONLY_KEYWORD = "EXCLUSIVEREADONLY";
    public const string NONEXCLUSIVEREADWRITE_KEYWORD = "NONEXCLUSIVEREADWRITE";
    public const string NONEXCLUSIVEREADONLY_KEYWORD = "NONEXCLUSIVEREADONLY";
    public const string SHAREDREADONLY_KEYWORD = "SHAREDREADONLY";
    public const string CONTEXT_KEYWORD = "CONTEXT";
    public const string CONNECTION_KEYWORD = "CONNECTION";
    public const string MIN_KEYWORD = "MIN";
    public const string POOL_KEYWORD = "POOL";
    public const string POOLING_KEYWORD = "POOLING";
    public const string PLUS_KEYWORD = "+";
    public const string MINUS_KEYWORD = "-";
    public const string UNARY_PLUS_KEYWORD = "+ UNARY";
    public const string UNARY_MINUS_KEYWORD = "- UNARY";
    public const string CAT_KEYWORD = "||";
    public const string EQUAL_KEYWORD = "=";
    public const string NOT_EQUAL_SQL_KEYWORD = "<>";
    public const string NOT_EQUAL_C_KEYWORD = "!=";
    public const string AND_KEYWORD = "AND";
    public const string OR_KEYWORD = "OR";
    public const string LESS_KEYWORD = "<";
    public const string LESS_OR_EQUAL_KEYWORD = "<=";
    public const string GREATER_KEYWORD = ">";
    public const string GREATER_OR_EQUAL_KEYWORD = ">=";
    public const string DIVIDE_KEYWORD = "/";
    public const string MOD_KEYWORD = "%";
    public const string IN_KEYWORD = "IN";
    public const string NOT_KEYWORD = "NOT";
    public const string UNARY_NOT_KEYWORD = "NOT UNARY";
    public const string LIKE_KEYWORD = "LIKE";
    public const string BETWEEN_KEYWORD = "BETWEEN";
    public const string IS_KEYWORD = "IS";
    public const string EXISTS_KEYWORD = "EXISTS";
    public const string UNARY_EXISTS_KEYWORD = "EXISTS UNARY";
    public const string BITWISE_NOT_KEYWORD = "~";
    public const string UNARY_BW_NOT_KEYWORD = "~ UNARY";
    public const string BITWISE_AND_KEYWORD = "&";
    public const string BITWISE_OR_KEYWORD = "|";
    public const string BITWISE_XOR_KEYWORD = "^";
    public const string MAX_KEYWORD = "MAX";
    public const string SHARP_KEYWORD = "#";
    public const string LOWER_FUNCTION = "LOWER";
    public const string UPPER_FUNCTION = "UPPER";
    public const string ASCII_FUNCTION = "ASCII";
    public const string UNICODE_FUNCTION = "UNICODE";
    public const string CHAR_FUNCTION = "CHAR";
    public const string NCHAR_FUNCTION = "NCHAR";
    public const string CHAR_INDEX_FUNCTION = "CHARINDEX";
    public const string LEN_FUNCTION = "LEN";
    public const string LTRIM_FUNCTION = "LTRIM";
    public const string RTRIM_FUNCTION = "RTRIM";
    public const string REVERSE_FUNCTION = "REVERSE";
    public const string SPACE_FUNCTION = "SPACE";
    public const string LEFT_FUNCTION = "LEFT";
    public const string RIGHT_FUNCTION = "RIGHT";
    public const string PATINDEX_FUNCTION = "PATINDEX";
    public const string REPLACE_FUNCTION = "REPLACE";
    public const string REPLICATE_FUNCTION = "REPLICATE";
    public const string STR_FUNCTION = "STR";
    public const string STUFF_FUNCTION = "STUFF";
    public const string SUBSTRING_FUNCTION = "SUBSTRING";
    public const string ABS_FUNCTION = "ABS";
    public const string ACOS_FUNCTION = "ACOS";
    public const string ASIN_FUNCTION = "ASIN";
    public const string ATAN_FUNCTION = "ATAN";
    public const string ATN2_FUNCTION = "ATN2";
    public const string CEILING_FUNCTION = "CEILING";
    public const string COS_FUNCTION = "COS";
    public const string COT_FUNCTION = "COT";
    public const string DEGREES_FUNCTION = "DEGREES";
    public const string EXP_FUNCTION = "EXP";
    public const string FLOOR_FUNCTION = "FLOOR";
    public const string FRAC_FUNCTION = "FRAC";
    public const string INT_FUNCTION = "INT";
    public const string LOG_FUNCTION = "LOG";
    public const string LOG10_FUNCTION = "LOG10";
    public const string MAXOF_FUNCTION = "MAXOF";
    public const string MINOF_FUNCTION = "MINOF";
    public const string PI_FUNCTION = "PI";
    public const string POWER_FUNCTION = "POWER";
    public const string RADIANS_FUNCTION = "RADIANS";
    public const string RAND_FUNCTION = "RAND";
    public const string ROUND_FUNCTION = "ROUND";
    public const string SIGN_FUNCTION = "SIGN";
    public const string SIN_FUNCTION = "SIN";
    public const string SQRT_FUNCTION = "SQRT";
    public const string SQUARE_FUNCTION = "SQUARE";
    public const string TAN_FUNCTION = "TAN";
    public const string SUM_FUNCTION = "SUM";
    public const string COUNT_FUNCTION = "COUNT";
    public const string COUNT_BIG_FUNCTION = "COUNT_BIG";
    public const string AVG_FUNCTION = "AVG";
    public const string MIN_FUNCTION = "MIN";
    public const string MAX_FUNCTION = "MAX";
    public const string STDEV_FUNCTION = "STDEV";
    public const string CAST_FUNCTION = "CAST";
    public const string ISNULL_FUNCTION = "ISNULL";
    public const string LOOKUP_FUNCTION = "LOOKUP";
    public const string NULLIF_FUNCTION = "NULLIF";
    public const string ISNUMMERIC_FUNCTION = "ISNUMERIC";
    public const string ISDATE_FUNCTION = "ISDATE";
    public const string CONVERT_FUNCTION = "CONVERT";
    public const string CASE_FUNCTION = "CASE";
    public const string CONTAINS_FUNCTION = "CONTAINS";
    public const string LASTIDENTITY_FUNCTION = "LASTIDENTITY";
    public const string COALESCE_FUNCTION = "COALESCE";
    public const string NEWID_FUNCTION = "NEWID";
    public const string IIF_FUNCTION = "IIF";
    public const string SP_COLUMNS_FUNCTION = "SP_COLUMNS";
    public const string SP_INDEXES_FUNCTION = "SP_INDEXES";
    public const string SP_FOREIGNKEYS_FUNCTION = "SP_FOREIGNKEYS";
    public const string SP_STORED_PROCEDURES = "SP_STORED_PROCEDURES";
    public const string SP_STORED_FUNCTIONS = "SP_UDF";
    public const string LASTTIMESTAMP_FUNCTION = "LASTTIMESTAMP";
    public const string LASTTABLEANCHOR = "LASTTABLEANCHOR";
    public const string DATEADD_FUNCTION = "DATEADD";
    public const string DATEDIFF_FUNCTION = "DATEDIFF";
    public const string DATENAME_FUNCTION = "DATENAME";
    public const string DATEPART_FUNCTION = "DATEPART";
    public const string DAY_FUNCTION = "DAY";
    public const string GETDATE_FUNCTION = "GETDATE";
    public const string GETUTCDATE_FUNCTION = "GETUTCDATE";
    public const string MONTH_FUNCTION = "MONTH";
    public const string YEAR_FUNCTION = "YEAR";
    public const string GETVIEWS_FUNCTION = "GETVIEWS";
    public const string GETVIEWCOLUMNS_FUNCTION = "GETVIEWCOLUMNS";
    public const string SP_VIEWS_FUNCTION = "SP_VIEWS";
    public const string SP_VIEWCOLUMNS_FUNCTION = "SP_VIEWCOLUMNS";
    public const string SP_RENAME_FUNCTION = "SP_RENAME";
    public const string AT_AT_IDENTITY_FUNCTION = "@@IDENTITY";
    public const string AT_AT_VERSION_FUNCTION = "@@VERSION";
    public const string AT_AT_DATABASE_GUID = "@@DATABASEID";
    public const string AT_AT_ERROR_VARIABLE = "@@ERROR";
    public const string AT_AT_ROWCOUNT = "@@ROWCOUNT";
    public const string AT_AT_TRANCOUNT = "@@TRANCOUNT";
    public const string YEAR_PART_KEYWORD = "YEAR";
    public const string YEAR_PART_KEYWORD2 = "YYYY";
    public const string YEAR_PART_KEYWORD3 = "YY";
    public const string QUARTER_PART_KEYWORD = "QUARTER";
    public const string QUARTER_PART_KEYWORD2 = "QQ";
    public const string QUARTER_PART_KEYWORD3 = "Q";
    public const string MONTH_PART_KEYWORD = "MONTH";
    public const string MONTH_PART_KEYWORD2 = "MM";
    public const string MONTH_PART_KEYWORD3 = "M";
    public const string DAYOFYEAR_PART_KEYWORD = "DAYOFYEAR";
    public const string DAYOFYEAR_PART_KEYWORD2 = "DY";
    public const string DAYOFYEAR_PART_KEYWORD3 = "Y";
    public const string DAY_PART_KEYWORD = "DAY";
    public const string DAY_PART_KEYWORD2 = "DD";
    public const string DAY_PART_KEYWORD3 = "D";
    public const string WEEK_PART_KEYWORD = "WEEK";
    public const string WEEK_PART_KEYWORD2 = "WK";
    public const string WEEK_PART_KEYWORD3 = "WW";
    public const string WEEKDAY_PART_KEYWORD = "WEEKDAY";
    public const string WEEKDAY_PART_KEYWORD2 = "DW";
    public const string WEEKDAY_PART_KEYWORD3 = "W";
    public const string HOUR_PART_KEYWORD = "HOUR";
    public const string HOUR_PART_KEYWORD2 = "HH";
    public const string MINUTE_PART_KEYWORD = "MINUTE";
    public const string MINUTE_PART_KEYWORD2 = "MI";
    public const string MINUTE_PART_KEYWORD3 = "N";
    public const string SECOND_PART_KEYWORD = "SECOND";
    public const string SECOND_PART_KEYWORD2 = "SS";
    public const string SECOND_PART_KEYWORD3 = "S";
    public const string MILLISECOND_PART_KEYWORD = "MILLISECOND";
    public const string MILLISECOND_PART_KEYWORD2 = "MS";
    public const string WHEN_KEYWORD = "WHEN";
    public const string THEN_KEYWORD = "THEN";
    public const string ELSE_KEYWORD = "ELSE";
    public const string END_KEYWORD = "END";
    public const string DBO_SCHEMA_NAME = "DBO";
    public const string IF_KEYWORD = "IF";
    public const string WITH_KEYWORD = "WITH";
    public const char RETURN_SYMBOL = '\r';
    public const char NEW_LINE_SYMBOL = '\n';
    public const char TAB_SYMBOL = '\t';
    public const char DOT_SYMBOL = '.';
    public const char LEFT_BRACKET_SYMBOL = '(';
    public const char RIGHT_BRACKET_SYMBOL = ')';
    public const char SINGLE_QUOTE_SYMBOL = '\'';
    public const char DOUBLE_QUOTE_SYMBOL = '"';
    public const char PLUS_SYMBOL = '+';
    public const char MINUS_SYMBOL = '-';
    public const char E_SYMBOL = 'e';
    public const char E_UPPER_SYMBOL = 'E';
    public const char LESS_SYMBOL = '<';
    public const char GREATER_SYMBOL = '>';
    public const char EM_SYMBOL = '!';
    public const char EQUAL_SYMBOL = '=';
    public const char VERT_LINE_SYMBOL = '|';
    public const char PERCENT_SYMBOL = '%';
    public const char UNDERSCORE_SYMBOL = '_';
    public const char LEFT_SQ_BRACKET_SYMBOL = '[';
    public const char RIGHT_SQ_BRACKET_SYMBOL = ']';
    public const char UPPER_ARROW_SYMBOL = '^';
    public const char SEMICOLON_SYMBOL = ';';
    public const char AT_SYMBOL = '@';
    public const char QUESTION = '?';
    public const char BINARY_SYMBOL = 'x';
    public const char BINARY_UPPER_SYMBOL = 'X';
    public const char ZERO_SYMBOL = '0';
    public const char SHARP_SYMBOL = '#';
    public const string DOUBLE_SINGLE_QUOTE = "''";
    public const string START_COMMENT = "/*";
    public const string END_COMMENT = "*/";
    public const string SINGLE_LINE_COMMENT = "--";
    public const string RAISEERROR_KEYWORD = "RAISERROR";
    public const string PRINT_KEYWORD = "PRINT";
    public const int COMMENT_LEN = 2;
    public const int DefaultWidth = 30;
    public const VistaDBType DefaultNumericType = VistaDBType.Float;
    public const string DEFAULT_DB_EXT = ".vdb4";
    public const string DEFAULT_FTS_NAME = "_FullTextIndex";
    public const string COLUMN_DIVIDER = ";";
    public const int MAX_PRIORITY = 6;
    private SQLParser.TokenValueClass tokenValue;
    private int symbolNo;
    private int rowNo;
    private int colNo;
    private int tokenLen;
    private bool suppressSkip;
    private string text;
    private int textLength;
    private Stack currentContext;
    private CreateTableStatement temporaryTable;
    private CultureInfo culture;
    private Statement parent;
    private static SQLParser.SpecialFunctionCollection specialFunctions;
    private static SQLParser.FunctionCollection builtInFunctions;
    private static SQLParser.OperatorCollection operators;

    static SQLParser()
    {
      SQLParser.typeNames.Add(SQLParser.CHAR_TYPE, SqlDataType.Char);
      SQLParser.typeNames.Add(SQLParser.CHARACTER_TYPE, SqlDataType.Char);
      SQLParser.typeNames.Add(SQLParser.NCHAR_TYPE, SqlDataType.NChar);
      SQLParser.typeNames.Add(SQLParser.VARCHAR_TYPE, SqlDataType.VarChar);
      SQLParser.typeNames.Add(SQLParser.NVARCHAR_TYPE, SqlDataType.NVarChar);
      SQLParser.typeNames.Add(SQLParser.TEXT_TYPE, SqlDataType.Text);
      SQLParser.typeNames.Add(SQLParser.NTEXT_TYPE, SqlDataType.NText);
      SQLParser.typeNames.Add(SQLParser.TINYINT_TYPE, SqlDataType.TinyInt);
      SQLParser.typeNames.Add(SQLParser.SMALLINT_TYPE, SqlDataType.SmallInt);
      SQLParser.typeNames.Add(SQLParser.INT_TYPE, SqlDataType.Int);
      SQLParser.typeNames.Add(SQLParser.INTEGER_TYPE, SqlDataType.Int);
      SQLParser.typeNames.Add(SQLParser.BIGINT_TYPE, SqlDataType.BigInt);
      SQLParser.typeNames.Add(SQLParser.REAL_TYPE, SqlDataType.Real);
      SQLParser.typeNames.Add(SQLParser.FLOAT_TYPE, SqlDataType.Float);
      SQLParser.typeNames.Add(SQLParser.DECIMAL_TYPE, SqlDataType.Decimal);
      SQLParser.typeNames.Add(SQLParser.DEC_TYPE, SqlDataType.Decimal);
      SQLParser.typeNames.Add(SQLParser.NUMERIC_TYPE, SqlDataType.Numeric);
      SQLParser.typeNames.Add(SQLParser.MONEY_TYPE, SqlDataType.Money);
      SQLParser.typeNames.Add(SQLParser.SMALLMONEY_TYPE, SqlDataType.SmallMoney);
      SQLParser.typeNames.Add(SQLParser.BIT_TYPE, SqlDataType.Bit);
      SQLParser.typeNames.Add(SQLParser.SMALLDATETIME_TYPE, SqlDataType.SmallDateTime);
      SQLParser.typeNames.Add(SQLParser.DATETIME_TYPE, SqlDataType.DateTime);
      SQLParser.typeNames.Add(SQLParser.IMAGE_TYPE, SqlDataType.Image);
      SQLParser.typeNames.Add(SQLParser.VARBINARY_TYPE, SqlDataType.VarBinary);
      SQLParser.typeNames.Add(SQLParser.BINARY_TYPE, SqlDataType.VarBinary);
      SQLParser.typeNames.Add(SQLParser.UNIQUEIDENTIFIER_TYPE, SqlDataType.UniqueIdentifier);
      SQLParser.typeNames.Add(SQLParser.TIMESTAMP_TYPE, SqlDataType.Timestamp);
      SQLParser.typeNames.Add(SQLParser.ROWVERSION_TYPE, SqlDataType.Timestamp);
      SQLParser.typeNames.Add(SQLParser.NATIONAL_TYPE_PART, SqlDataType.National);
      SQLParser.typeNames.Add("TABLE", SqlDataType.Table);
      SQLParser.InitReservedWords();
      SQLParser.specialFunctions = new SQLParser.SpecialFunctionCollection();
      FunctionDescr functionDescr1 = (FunctionDescr) new GetViewsFunctionDescr();
      SQLParser.specialFunctions.Add("GETVIEWS", functionDescr1);
      SQLParser.specialFunctions.Add("SP_VIEWS", functionDescr1);
      FunctionDescr functionDescr2 = (FunctionDescr) new GetViewColumnsFunctionDescr();
      SQLParser.specialFunctions.Add("GETVIEWCOLUMNS", functionDescr2);
      SQLParser.specialFunctions.Add("SP_VIEWCOLUMNS", functionDescr2);
      FunctionDescr functionDescr3 = (FunctionDescr) new SpColumnsFunctionDescr();
      SQLParser.specialFunctions.Add("SP_COLUMNS", functionDescr3);
      FunctionDescr functionDescr4 = (FunctionDescr) new SpIndexesFunctionDescr();
      SQLParser.specialFunctions.Add("SP_INDEXES", functionDescr4);
      FunctionDescr functionDescr5 = (FunctionDescr) new SpStoredProceduresDescr();
      SQLParser.specialFunctions.Add(nameof (SP_STORED_PROCEDURES), functionDescr5);
      FunctionDescr functionDescr6 = (FunctionDescr) new SpStoredFunctionDesr();
      SQLParser.specialFunctions.Add("SP_UDF", functionDescr6);
      FunctionDescr functionDescr7 = (FunctionDescr) new SpForeignKeysFunctionDescr();
      SQLParser.specialFunctions.Add("SP_FOREIGNKEYS", functionDescr7);
      SQLParser.builtInFunctions = new SQLParser.FunctionCollection();
      foreach (string key in SQLParser.specialFunctions.Keys)
        SQLParser.builtInFunctions.Add(key, SQLParser.specialFunctions[key]);
      SQLParser.builtInFunctions.Add("LOWER", (FunctionDescr) new LowerFunctionDescr());
      SQLParser.builtInFunctions.Add("UPPER", (FunctionDescr) new UpperFunctionDescr());
      SQLParser.builtInFunctions.Add("ASCII", (FunctionDescr) new ASCIIFunctionDescr());
      SQLParser.builtInFunctions.Add("UNICODE", (FunctionDescr) new UnicodeFunctionDescr());
      SQLParser.builtInFunctions.Add("CHAR", (FunctionDescr) new CharFunctionDescr());
      SQLParser.builtInFunctions.Add("NCHAR", (FunctionDescr) new NCharFunctionDescr());
      SQLParser.builtInFunctions.Add("CHARINDEX", (FunctionDescr) new CharIndexFunctionDescr());
      SQLParser.builtInFunctions.Add("LEN", (FunctionDescr) new LenFunctionDescr());
      SQLParser.builtInFunctions.Add("LTRIM", (FunctionDescr) new LTrimFunctionDescr());
      SQLParser.builtInFunctions.Add("RTRIM", (FunctionDescr) new RTrimFunctionDescr());
      SQLParser.builtInFunctions.Add("REVERSE", (FunctionDescr) new ReverseFunctionDescr());
      SQLParser.builtInFunctions.Add("SPACE", (FunctionDescr) new SpaceFunctionDescr());
      SQLParser.builtInFunctions.Add("LEFT", (FunctionDescr) new LeftFunctionDescr());
      SQLParser.builtInFunctions.Add("RIGHT", (FunctionDescr) new RightFunctionDescr());
      SQLParser.builtInFunctions.Add("REPLACE", (FunctionDescr) new ReplaceFunctionDescr());
      SQLParser.builtInFunctions.Add("REPLICATE", (FunctionDescr) new ReplicateFunctionDescr());
      SQLParser.builtInFunctions.Add("STR", (FunctionDescr) new StrFunctionDescr());
      SQLParser.builtInFunctions.Add("STUFF", (FunctionDescr) new StuffFunctionDescr());
      SQLParser.builtInFunctions.Add("SUBSTRING", (FunctionDescr) new SubStringFunctionDescr());
      SQLParser.builtInFunctions.Add("PATINDEX", (FunctionDescr) new PAtIndexFunctionDescr());
      SQLParser.builtInFunctions.Add("ABS", (FunctionDescr) new AbsFunctionDescr());
      SQLParser.builtInFunctions.Add("ACOS", (FunctionDescr) new ACosFunctionDescr());
      SQLParser.builtInFunctions.Add("ASIN", (FunctionDescr) new ASinFunctionDescr());
      SQLParser.builtInFunctions.Add("ATAN", (FunctionDescr) new ATanFunctionDescr());
      SQLParser.builtInFunctions.Add("ATN2", (FunctionDescr) new ATN2FunctionDescr());
      SQLParser.builtInFunctions.Add("CEILING", (FunctionDescr) new CeilingFunctionDescr());
      SQLParser.builtInFunctions.Add("COS", (FunctionDescr) new CosFunctionDescr());
      SQLParser.builtInFunctions.Add("COT", (FunctionDescr) new CotFunctionDescr());
      SQLParser.builtInFunctions.Add("DEGREES", (FunctionDescr) new DegreesFunctionDescr());
      SQLParser.builtInFunctions.Add("EXP", (FunctionDescr) new ExpFunctionDescr());
      SQLParser.builtInFunctions.Add("FLOOR", (FunctionDescr) new FloorFunctionDescr());
      SQLParser.builtInFunctions.Add("FRAC", (FunctionDescr) new FracFunctionDescr());
      SQLParser.builtInFunctions.Add("INT", (FunctionDescr) new IntFunctionDescr());
      SQLParser.builtInFunctions.Add("LOG", (FunctionDescr) new LogFunctionDescr());
      SQLParser.builtInFunctions.Add("LOG10", (FunctionDescr) new Log10FunctionDescr());
      SQLParser.builtInFunctions.Add("MAXOF", (FunctionDescr) new MaxOfFunctionDescr());
      SQLParser.builtInFunctions.Add("MINOF", (FunctionDescr) new MinOfFunctionDescr());
      SQLParser.builtInFunctions.Add("PI", (FunctionDescr) new PIFunctionDescr());
      SQLParser.builtInFunctions.Add("POWER", (FunctionDescr) new PowerFunctionDescr());
      SQLParser.builtInFunctions.Add("RADIANS", (FunctionDescr) new RadiansFunctionDescr());
      SQLParser.builtInFunctions.Add("RAND", (FunctionDescr) new RandFunctionDescr());
      SQLParser.builtInFunctions.Add("ROUND", (FunctionDescr) new RoundFunctionDescr());
      SQLParser.builtInFunctions.Add("SIGN", (FunctionDescr) new SignFunctionDescr());
      SQLParser.builtInFunctions.Add("SIN", (FunctionDescr) new SinFunctionDescr());
      SQLParser.builtInFunctions.Add("SQRT", (FunctionDescr) new SqrtFunctionDescr());
      SQLParser.builtInFunctions.Add("SQUARE", (FunctionDescr) new SquareFunctionDescr());
      SQLParser.builtInFunctions.Add("TAN", (FunctionDescr) new TanFunctionDescr());
      SQLParser.builtInFunctions.Add("SUM", (FunctionDescr) new SumFunctionDescr());
      SQLParser.builtInFunctions.Add("COUNT", (FunctionDescr) new CountFunctionDescr());
      SQLParser.builtInFunctions.Add("COUNT_BIG", (FunctionDescr) new CountBigFunctionDescr());
      SQLParser.builtInFunctions.Add("AVG", (FunctionDescr) new AvgFunctionDescr());
      SQLParser.builtInFunctions.Add("MIN", (FunctionDescr) new MinFunctionDescr());
      SQLParser.builtInFunctions.Add("MAX", (FunctionDescr) new MaxFunctionDescr());
      SQLParser.builtInFunctions.Add("STDEV", (FunctionDescr) new StDevFunctionDescr());
      SQLParser.builtInFunctions.Add("CAST", (FunctionDescr) new CastFunctionDescr());
      SQLParser.builtInFunctions.Add("ISNULL", (FunctionDescr) new IsNullFunctionDescr());
      SQLParser.builtInFunctions.Add("LOOKUP", (FunctionDescr) new LookupFunctionDescr());
      SQLParser.builtInFunctions.Add("NULLIF", (FunctionDescr) new NullIfFunctionDescr());
      SQLParser.builtInFunctions.Add("ISNUMERIC", (FunctionDescr) new IsNumericFunctionDescr());
      SQLParser.builtInFunctions.Add("CONVERT", (FunctionDescr) new ConvertFunctionDescr());
      SQLParser.builtInFunctions.Add("CASE", (FunctionDescr) new CaseFunctionDescr());
      SQLParser.builtInFunctions.Add("LASTIDENTITY", (FunctionDescr) new LastIdentityFunctionDescr());
      SQLParser.builtInFunctions.Add("CONTAINS", (FunctionDescr) new ContainsFunctionDescr());
      SQLParser.builtInFunctions.Add("COALESCE", (FunctionDescr) new CoalesceFunctionDescr());
      SQLParser.builtInFunctions.Add("NEWID", (FunctionDescr) new NewIDFunctionDescr());
      SQLParser.builtInFunctions.Add("SP_RENAME", (FunctionDescr) new RenameFunctionDescr());
      SQLParser.builtInFunctions.Add("IIF", (FunctionDescr) new IIFFunctionDescr());
      SQLParser.builtInFunctions.Add("LASTTIMESTAMP", (FunctionDescr) new LastTimestampFunctionDescr());
      SQLParser.builtInFunctions.Add(nameof (LASTTABLEANCHOR), (FunctionDescr) new LastTableAnchorDesc());
      SQLParser.builtInFunctions.Add("DATEADD", (FunctionDescr) new DateAddFunctionDescr());
      SQLParser.builtInFunctions.Add("DATEDIFF", (FunctionDescr) new DateDiffFunctionDescr());
      SQLParser.builtInFunctions.Add("DATENAME", (FunctionDescr) new DateNameFunctionDescr());
      SQLParser.builtInFunctions.Add("DATEPART", (FunctionDescr) new DatePartFunctionDescr());
      SQLParser.builtInFunctions.Add("DAY", (FunctionDescr) new DayFunctionDescr());
      SQLParser.builtInFunctions.Add("GETDATE", (FunctionDescr) new GetDateFunctionDescr());
      SQLParser.builtInFunctions.Add("GETUTCDATE", (FunctionDescr) new GetUtcDateFunctionDescr());
      SQLParser.builtInFunctions.Add("MONTH", (FunctionDescr) new MonthFunctionDescr());
      SQLParser.builtInFunctions.Add("YEAR", (FunctionDescr) new YearFunctionDescr());
      SQLParser.builtInFunctions.Add("@@IDENTITY", (FunctionDescr) new IdentityVariableDescr());
      SQLParser.builtInFunctions.Add("@@VERSION", (FunctionDescr) new VistaDBVersionDescr());
      SQLParser.builtInFunctions.Add("@@ERROR", (FunctionDescr) new VistaDBErrorVariableDescription());
      SQLParser.builtInFunctions.Add("@@DATABASEID", (FunctionDescr) new VistaDBDatabaseIdVariableDescriptor());
      SQLParser.builtInFunctions.Add("@@ROWCOUNT", (FunctionDescr) new VistaDBRowCountVariableDescription());
      SQLParser.builtInFunctions.Add("@@TRANCOUNT", (FunctionDescr) new VistaDBTranCountVariableDescription());
      SQLParser.operators = new SQLParser.OperatorCollection();
      SQLParser.operators.Add("EXISTS UNARY", (IOperatorDescr) new ExistsOperatorDescr());
      SQLParser.operators.Add("~ UNARY", (IOperatorDescr) new BitwiseNotOperatorDescr());
      SQLParser.operators.Add("*", (IOperatorDescr) new MultiplyOperatorDescr());
      SQLParser.operators.Add("/", (IOperatorDescr) new DivideOperatorDescr());
      SQLParser.operators.Add("%", (IOperatorDescr) new ModOperatorDescr());
      SQLParser.operators.Add("- UNARY", (IOperatorDescr) new UnaryMinusOperatorDescr());
      SQLParser.operators.Add("+ UNARY", (IOperatorDescr) new UnaryPlusOperatorDescr());
      SQLParser.operators.Add("+", (IOperatorDescr) new PlusOperatorDescr());
      SQLParser.operators.Add("-", (IOperatorDescr) new MinusOperatorDescr());
      SQLParser.operators.Add("&", (IOperatorDescr) new BitwiseAndOperatorDescr());
      SQLParser.operators.Add("|", (IOperatorDescr) new BitwiseOrOperatorDescr());
      SQLParser.operators.Add("^", (IOperatorDescr) new BitwiseXorOperatorDescr());
      SQLParser.operators.Add("=", (IOperatorDescr) new EqualOperatorDescr());
      IOperatorDescr operatorDescr = (IOperatorDescr) new NotEqualOperatorDescr();
      SQLParser.operators.Add("!=", operatorDescr);
      SQLParser.operators.Add("<>", operatorDescr);
      SQLParser.operators.Add("<", (IOperatorDescr) new LessThanOperatorDescr());
      SQLParser.operators.Add("<=", (IOperatorDescr) new LessOrEqualOperatorDescr());
      SQLParser.operators.Add(">", (IOperatorDescr) new GreaterThanOperatorDescr());
      SQLParser.operators.Add(">=", (IOperatorDescr) new GreaterOrEqualOperatorDescr());
      SQLParser.operators.Add("IN", (IOperatorDescr) new InOperatorDescr());
      SQLParser.operators.Add("LIKE", (IOperatorDescr) new LikeOperatorDescr());
      SQLParser.operators.Add("BETWEEN", (IOperatorDescr) new BetweenOperatorDescr());
      SQLParser.operators.Add("IS", (IOperatorDescr) new IsNullOperatorDescr());
      SQLParser.operators.Add("NOT", (IOperatorDescr) new NotBaseOperatorDescr());
      SQLParser.operators.Add("NOT UNARY", (IOperatorDescr) new NotOperatorDescr());
      SQLParser.operators.Add("AND", (IOperatorDescr) new AndOperatorDescr());
      SQLParser.operators.Add("OR", (IOperatorDescr) new OrOperatorDescr());
    }

    internal static SQLParser CreateInstance(string text, CultureInfo culture)
    {
      return new SQLParser(text, culture);
    }

    private SQLParser(string text, CultureInfo culture)
    {
      this.parent = (Statement) null;
      this.tokenValue = new SQLParser.TokenValueClass();
      this.currentContext = new Stack();
      this.culture = culture;
      this.SetText(text);
    }

    public void SetText(string text)
    {
      this.text = text;
      this.textLength = this.text.Length;
      this.symbolNo = 0;
      this.rowNo = 1;
      this.colNo = 1;
    }

    public Statement Parent
    {
      get
      {
        return this.parent;
      }
      set
      {
        this.parent = value;
      }
    }

    public string Text
    {
      get
      {
        return this.text;
      }
    }

    public int SymbolNo
    {
      get
      {
        return this.symbolNo;
      }
    }

    public bool EndOfText
    {
      get
      {
        return this.tokenValue.Token == null;
      }
    }

    public CultureInfo Culture
    {
      get
      {
        return this.culture;
      }
      set
      {
        this.culture = value;
      }
    }

    internal CurrentTokenContext Context
    {
      get
      {
        return this.currentContext.Peek() as CurrentTokenContext;
      }
    }

    internal void PushContext(CurrentTokenContext context)
    {
      this.currentContext.Push((object) context);
    }

    internal void PopContext()
    {
      this.currentContext.Pop();
    }

    private bool CheckUdfContext(string udfName)
    {
      foreach (CurrentTokenContext currentTokenContext in this.currentContext)
      {
        if (currentTokenContext.ContextType == CurrentTokenContext.TokenContext.StoredFunction)
          return currentTokenContext.ContextName.CompareTo(udfName) == 0;
      }
      return false;
    }

    internal Signature NextSignature(bool needSkip, bool raiseException, int priority)
    {
      if (needSkip && !this.SkipToken(raiseException))
        return (Signature) null;
      Signature signature = priority == -1 ? this.ParseExpressions() : this.ParsePriority(priority);
      if (raiseException && signature == (Signature) null)
        throw new VistaDBSQLException(502, "end of text", this.rowNo, this.symbolNo + 1);
      return signature;
    }

    internal SQLParser.TokenValueClass TokenValue
    {
      get
      {
        return this.tokenValue;
      }
    }

    internal void ExpectedExpression(string expression, params string[] alternative)
    {
      if (this.tokenValue.TokenType == TokenType.String || string.Compare(expression, this.tokenValue.Token, StringComparison.OrdinalIgnoreCase) != 0)
      {
        if (alternative != null)
        {
          foreach (string str in alternative)
          {
            expression += ", ";
            expression += str;
          }
        }
        throw new VistaDBSQLException(507, expression, this.tokenValue.RowNo, this.tokenValue.ColNo);
      }
    }

    internal bool IsToken(string expression)
    {
      if (this.tokenValue.TokenType != TokenType.String)
        return string.Compare(this.tokenValue.Token, expression, StringComparison.OrdinalIgnoreCase) == 0;
      return false;
    }

    internal bool TokenEndsWith(string expression)
    {
      if (this.tokenValue.TokenType != TokenType.String)
        return this.tokenValue.Token.EndsWith(expression, StringComparison.OrdinalIgnoreCase);
      return false;
    }

    internal bool TokenIsSystemFunction()
    {
      return SQLParser.builtInFunctions.ContainsKey(this.TokenValue.Token);
    }

    internal ITableValuedFunction CreateSpecialFunction(string name, int rowNo, int colNo, int symbolNo)
    {
      this.tokenValue.SetPosition(rowNo, colNo, symbolNo);
      FunctionDescr specialFunction = SQLParser.specialFunctions[name];
      if (specialFunction != null)
        return (ITableValuedFunction) specialFunction.CreateSignature(this);
      IUserDefinedFunctionInformation userDefinedFunction = this.parent.Database.GetUserDefinedFunctions()[name];
      if (userDefinedFunction == null)
        return (ITableValuedFunction) new CLRResultSetFunction(this, name);
      if (userDefinedFunction.ScalarValued)
        throw new Exception("Scalar-valued function can't be executed from exec");
      return (ITableValuedFunction) new TableValuedFunction(this, userDefinedFunction);
    }

    internal void CheckVariableName()
    {
      string token = this.TokenValue.Token;
      if (token[0] != '@')
        throw new VistaDBSQLException(500, "@", this.TokenValue.RowNo, this.TokenValue.ColNo);
      if (token.Length == 1)
        throw new VistaDBSQLException(621, token, this.TokenValue.RowNo, this.TokenValue.ColNo);
    }

    internal bool SkipSemicolons()
    {
      bool endOfText;
      for (endOfText = this.EndOfText; !endOfText && this.IsToken(";"); endOfText = this.EndOfText)
        this.SkipToken(false);
      return endOfText;
    }

    public void SuppressNextSkipToken()
    {
      this.suppressSkip = true;
    }

    public bool SkipToken(bool raiseException)
    {
      int symbolNo1 = this.symbolNo;
      if (this.suppressSkip)
      {
        this.suppressSkip = false;
        if (this.tokenValue.Token != null)
          return true;
        if (raiseException)
          throw new VistaDBSQLException(502, "end of text", this.rowNo, this.symbolNo + 1);
        return false;
      }
      do
        ;
      while (this.SkipComments());
      if (this.textLength == this.symbolNo)
      {
        this.tokenValue.SetToken(0, 0, 0, (string) null, TokenType.Unknown);
        if (raiseException)
          throw new VistaDBSQLException(502, "end of text", this.rowNo, this.symbolNo + 1);
        return false;
      }
      char c = this.text[this.symbolNo];
      int symbolNo2 = this.symbolNo;
      int rowNo = this.rowNo;
      int colNo = this.colNo;
      this.tokenLen = 0;
      bool doNotCheckAlias = false;
      TokenType tokenType;
      if (c == '0' && this.textLength > this.symbolNo + 1 && (this.text[this.symbolNo + 1] == 'x' || this.text[this.symbolNo + 1] == 'X'))
        tokenType = this.ReadBinary();
      else if (SQLParser.IsNumeric(c) || c == '.' && this.textLength > this.symbolNo + 1 && SQLParser.IsNumeric(this.text[this.symbolNo + 1]))
        tokenType = this.ReadNumeric(c);
      else if (SQLParser.IsLetter(c))
      {
        tokenType = this.ReadUnknown(c, ref doNotCheckAlias);
      }
      else
      {
        switch (c)
        {
          case '"':
          case '[':
            tokenType = this.ReadName(c, ref doNotCheckAlias);
            break;
          case '\'':
            tokenType = this.ReadString(c);
            break;
          default:
            tokenType = this.ReadSpecialSymbol(c);
            break;
        }
      }
      string token;
      switch (tokenType)
      {
        case TokenType.String:
          token = this.text.Substring(symbolNo2 + 1, this.tokenLen - 2).Replace("''", '\''.ToString());
          break;
        case TokenType.Name:
          token = this.text.Substring(symbolNo2 + 1, this.tokenLen - 2);
          break;
        default:
          token = this.text.Substring(symbolNo2, this.tokenLen);
          break;
      }
      this.tokenValue.SetToken(rowNo, colNo, symbolNo1, token, tokenType, doNotCheckAlias);
      return true;
    }

    public string GetStringUntil(char stopChar)
    {
      do
        ;
      while (this.SkipComments());
      if (this.text[this.symbolNo] == '\'')
      {
        if (!this.SkipToken(false))
          return (string) null;
        return this.tokenValue.Token;
      }
      int symbolNo = this.symbolNo;
      while (this.textLength > this.symbolNo && (int) this.text[this.symbolNo] != (int) stopChar)
        this.IncrementSymbolNo(true);
      if (symbolNo == this.symbolNo)
        return (string) null;
      return this.text.Substring(symbolNo, this.symbolNo - symbolNo);
    }

    private TokenType ReadBinary()
    {
      this.IncrementSymbolNo(false);
      do
        ;
      while (this.IncrementSymbolNo(false) && SQLParser.IsHex(this.text[this.symbolNo]));
      return TokenType.Binary;
    }

    private TokenType ReadString(char c)
    {
      bool flag = false;
      if (this.IncrementSymbolNo(true) || this.textLength != this.symbolNo)
      {
        for (c = this.text[this.symbolNo]; c != '\'' || this.IncrementSymbolNo(false) && this.text[this.symbolNo] == '\''; c = this.text[this.symbolNo])
        {
          if (!this.IncrementSymbolNo(true) && this.textLength == this.symbolNo)
            goto label_6;
        }
        flag = true;
      }
label_6:
      if (!flag)
        throw new VistaDBSQLException(503, "", this.rowNo, this.colNo + 1);
      return TokenType.String;
    }

    private TokenType ReadNumeric(char c)
    {
      TokenType tokenType = c == '.' ? TokenType.Float : TokenType.Integer;
      if (this.IncrementSymbolNo(false))
      {
        for (c = this.text[this.symbolNo]; SQLParser.IsNumericExt(c); c = this.text[this.symbolNo])
        {
          switch (c)
          {
            case '+':
            case '-':
              c = this.text[this.symbolNo - 1];
              if (c == 'e' || c == 'E')
                break;
              goto label_9;
            case '.':
              if (tokenType != TokenType.Float)
              {
                tokenType = TokenType.Float;
                break;
              }
              goto label_9;
          }
          if (!this.IncrementSymbolNo(false))
            break;
        }
      }
label_9:
      return tokenType;
    }

    private TokenType ReadUnknown(char c, ref bool doNotCheckAlias)
    {
      if (this.IncrementSymbolNo(false))
      {
        char ch = char.MinValue;
        c = this.text[this.symbolNo];
        while ((SQLParser.IsLetter(c) || SQLParser.IsNumeric(c) || ch == '.') && this.IncrementSymbolNo(false))
        {
          ch = c;
          c = this.text[this.symbolNo];
          if (ch == '.' && (c == '[' || c == '"'))
          {
            int num = (int) this.ReadName(c, ref doNotCheckAlias);
            return TokenType.ComplexName;
          }
        }
      }
      doNotCheckAlias = false;
      return TokenType.Unknown;
    }

    private TokenType ReadSpecialSymbol(char c)
    {
      TokenType tokenType;
      if (c == '(')
      {
        tokenType = TokenType.LeftBracket;
      }
      else
      {
        switch (c)
        {
          case '!':
            if (this.textLength > this.symbolNo + 1 && this.text[this.symbolNo + 1] == '=')
            {
              this.IncrementSymbolNo(false);
              break;
            }
            break;
          case '<':
            if (this.textLength > this.symbolNo + 1 && (this.text[this.symbolNo + 1] == '>' || this.text[this.symbolNo + 1] == '='))
            {
              this.IncrementSymbolNo(false);
              break;
            }
            break;
          case '>':
            if (this.textLength > this.symbolNo + 1 && this.text[this.symbolNo + 1] == '=')
            {
              this.IncrementSymbolNo(false);
              break;
            }
            break;
          case '|':
            if (this.textLength > this.symbolNo + 1 && this.text[this.symbolNo + 1] == '|')
            {
              this.IncrementSymbolNo(false);
              break;
            }
            break;
        }
        tokenType = TokenType.Unknown;
      }
      this.IncrementSymbolNo(false);
      return tokenType;
    }

    private TokenType ReadName(char c, ref bool doNotCheckAlias)
    {
      char ch1 = c == '"' ? c : ']';
      while (this.IncrementSymbolNo(false))
      {
        c = this.text[this.symbolNo];
        if ((int) c == (int) ch1)
        {
          doNotCheckAlias = true;
          if (!this.IncrementSymbolNo(false) || this.text[this.symbolNo] != '.')
            return TokenType.Name;
          if (!this.IncrementSymbolNo(false))
            throw new VistaDBSQLException(505, "", this.rowNo, this.colNo + 1);
          c = this.text[this.symbolNo];
          if (c == '[' || c == '"')
          {
            char ch2 = c == '"' ? c : ']';
            while (this.IncrementSymbolNo(false))
            {
              c = this.text[this.symbolNo];
              if ((int) c == (int) ch2)
              {
                this.IncrementSymbolNo(false);
                goto label_15;
              }
            }
            throw new VistaDBSQLException(505, "", this.rowNo, this.colNo + 1);
          }
          int num = (int) this.ReadUnknown(c, ref doNotCheckAlias);
label_15:
          return TokenType.ComplexName;
        }
      }
      throw new VistaDBSQLException(504, "", this.rowNo, this.colNo + 1);
    }

    private bool SkipComments()
    {
      this.SkipSpaces();
      if (this.textLength <= this.symbolNo + 2 - 1)
        return false;
      string strA = this.text.Substring(this.symbolNo, 2);
      if (string.Compare(strA, "/*", StringComparison.OrdinalIgnoreCase) == 0)
      {
        this.symbolNo += 2;
        while (this.textLength > this.symbolNo + "*/".Length - 1)
        {
          if (string.Compare(this.text.Substring(this.symbolNo, "/*".Length), "*/", StringComparison.OrdinalIgnoreCase) == 0)
          {
            this.symbolNo += "*/".Length;
            this.SkipSpaces();
            return true;
          }
          this.IncrementSymbolNo(false);
        }
        throw new VistaDBSQLException(506, "", this.rowNo, this.colNo + 1);
      }
      if (string.Compare(strA, "--", StringComparison.OrdinalIgnoreCase) == 0)
      {
        this.symbolNo += 2;
        for (; this.textLength > this.symbolNo; ++this.symbolNo)
        {
          switch (this.text[this.symbolNo])
          {
            case '\n':
            case '\r':
              this.SkipSpaces();
              return true;
            default:
              continue;
          }
        }
      }
      return false;
    }

    private void SkipSpaces()
    {
      while (this.textLength > this.symbolNo && (this.text[this.symbolNo] == '\t' || this.text[this.symbolNo] <= ' '))
        this.IncrementSymbolNo(false);
    }

    private bool IncrementSymbolNo(bool addNewLineLen)
    {
      ++this.symbolNo;
      ++this.tokenLen;
      if (this.textLength == this.symbolNo)
        return false;
      switch (this.text[this.symbolNo])
      {
        case '\t':
          this.colNo += 4 - this.colNo % 4;
          return false;
        case '\n':
          ++this.symbolNo;
          if (addNewLineLen)
            ++this.tokenLen;
          ++this.rowNo;
          this.colNo = 1;
          return false;
        case '\r':
          ++this.symbolNo;
          if (addNewLineLen)
            ++this.tokenLen;
          ++this.rowNo;
          this.colNo = 1;
          if (this.textLength > this.symbolNo && this.text[this.symbolNo] == '\n')
          {
            ++this.symbolNo;
            if (addNewLineLen)
              ++this.tokenLen;
          }
          return false;
        default:
          ++this.colNo;
          return true;
      }
    }

    private static bool IsLetter(char c)
    {
      if (c != '_' && (c < 'A' || c > 'Z') && ((c < 'a' || c > 'z') && c != '.'))
        return c == '@';
      return true;
    }

    private static bool IsNumeric(char c)
    {
      if (c >= '0')
        return c <= '9';
      return false;
    }

    private static bool IsNumericExt(char c)
    {
      if (!IsNumeric(c) && c != 'e' && (c != 'E' && c != '-') && c != '+')
        return c == '.';
      return true;
    }

    private static bool IsHex(char c)
    {
      if (IsNumeric(c) || c >= 'A' && c <= 'F')
        return true;
      if (c >= 'a')
        return c <= 'f';
      return false;
    }

    private Signature ParsePriority(int priority)
    {
      Signature leftSignature;
      string index;
      if (this.tokenValue.TokenType != TokenType.Unknown)
      {
        leftSignature = this.ParseExpressions();
        index = this.tokenValue.Token;
      }
      else
      {
        leftSignature = (Signature) null;
        index = this.tokenValue.Token + " UNARY";
      }
      int num = 0;
      while (this.tokenValue.TokenType == TokenType.Unknown && !this.EndOfText)
      {
        IOperatorDescr operatorDescr = operators[index];
        if (operatorDescr == null || operatorDescr.Priority < num || operatorDescr.Priority > priority)
        {
          if (leftSignature != (Signature) null)
            return leftSignature;
          leftSignature = this.ParseExpressions();
        }
        else
        {
          leftSignature = operatorDescr.CreateSignature(leftSignature, this);
          num = operatorDescr.Priority;
        }
        index = this.tokenValue.Token;
      }
      return leftSignature;
    }

    private Signature ParseExpressions()
    {
      bool flag = true;
      Signature signature = (Signature) ConstantSignature.CreateSignature(this);
      if (signature == (Signature) null)
      {
        switch (this.tokenValue.TokenType)
        {
          case TokenType.Unknown:
            string token = this.tokenValue.Token;
            if (ParameterSignature.IsParameter(token))
            {
              signature = ParameterSignature.CreateSignature(this);
              break;
            }
            if (SubQuerySignature.IsSubQuery(token))
            {
              signature = SubQuerySignature.CreateSignature(this);
              flag = false;
              break;
            }
            signature = this.ParseFunctions(true);
            if (signature == (Signature) null)
            {
              signature = (Signature) ColumnSignature.CreateSignature(this);
              break;
            }
            break;
          case TokenType.LeftBracket:
            this.SkipToken(true);
            signature = this.ParsePriority(6);
            this.ExpectedExpression(")");
            break;
          case TokenType.Name:
          case TokenType.ComplexName:
            signature = this.ParseFunctions(false);
            if (!(signature != (Signature) null))
            {
              signature = (Signature) ColumnSignature.CreateSignature(this);
              break;
            }
            break;
          default:
            signature = (Signature) null;
            break;
        }
      }
      if (flag)
        this.SkipToken(false);
      return signature;
    }

    private Signature ParseFunctions(bool includeSystem)
    {
      string token = this.tokenValue.Token;
      FunctionDescr functionDescr = includeSystem ? builtInFunctions[token] : (FunctionDescr) null;
      int symbolNo1 = this.symbolNo;
      int rowNo1 = this.rowNo;
      int colNo1 = this.colNo;
      int symbolNo2 = this.tokenValue.SymbolNo;
      int rowNo2 = this.tokenValue.RowNo;
      int colNo2 = this.tokenValue.ColNo;
      bool validateName = this.tokenValue.ValidateName;
      TokenType tokenType = this.tokenValue.TokenType;
      this.SkipToken(false);
      if (this.parent.Connection.Database != null)
      {
        string index = token;
        if (tokenType != TokenType.Name)
        {
          int startIndex = 0;
          string namePart = GetNamePart(ref startIndex, token, rowNo1, colNo1);
          if (startIndex != -1 && string.Compare(namePart, "DBO", StringComparison.OrdinalIgnoreCase) == 0)
          {
            index = GetNamePart(ref startIndex, token, rowNo1, colNo1);
            if (startIndex != -1)
              index = token;
          }
        }
        if (this.IsToken("("))
        {
          IUserDefinedFunctionCollection definedFunctions = this.parent.Connection.Database.GetUserDefinedFunctions();
          if (definedFunctions.ContainsKey(index))
            functionDescr = (FunctionDescr) new StoredFunctionDescr(definedFunctions[index]);
          else if (this.CheckUdfContext(index))
          {
            this.SkipQuotes();
            return this.NextSignature(true, true, 6);
          }
        }
        else
        {
          IStoredProcedureCollection storedProcedures = parent.Connection.Database.GetStoredProcedures();
          if (storedProcedures.ContainsKey(index))
            functionDescr = (FunctionDescr) new StoredProcedureDescr(storedProcedures[index]);
        }
        if (parent.Connection.Database.TryGetProcedure(index, out ClrHosting.ClrProcedure procedure))
          return (Signature) new CLRStoredProcedure(this, index);
        if (this.parent.Connection.Database.GetClrProcedures().ContainsKey(index))
          return (Signature) new CLRStoredProcedure(this, index);
      }
      this.SetPosition(rowNo1, colNo1, symbolNo1, rowNo2, colNo2, symbolNo2, token, tokenType, !validateName);
      return functionDescr?.CreateSignature(this);
    }

    private void SetPosition(int rowNo, int colNo, int symbolNo, int tokenRowNo, int tokenColNo, int tokenSymbolNo, string token, TokenType tokenType, bool doNotCheckAlias)
    {
      this.rowNo = rowNo;
      this.colNo = colNo;
      this.symbolNo = symbolNo;
      this.tokenValue.SetToken(tokenRowNo, tokenColNo, tokenSymbolNo, token, tokenType, doNotCheckAlias);
    }

    public VistaDBType ReadDataType(out int len)
    {
      SqlDataType sqlDataType = this.ReadSqlDataType();
      len = 0;
      if (!this.IsToken("("))
        return sqlToNativeDataType[(int) sqlDataType];
      VistaDBType dataType;
      len = this.ReadDataTypeLen(sqlDataType, out dataType);
      if (this.IsToken(")"))
      {
        this.SkipToken(false);
        return dataType;
      }
      this.ExpectedExpression(",");
      this.ReadDataTypeScale(sqlDataType);
      this.ExpectedExpression(")");
      this.SkipToken(false);
      return dataType;
    }

    private SqlDataType ReadSqlDataType()
    {
      this.temporaryTable = (CreateTableStatement) null;
      string token = this.tokenValue.Token;
      if (!typeNames.ContainsKey(token))
        throw new VistaDBSQLException(624, "Expected to find Sql Data Type", this.tokenValue.RowNo, this.tokenValue.ColNo);
      SqlDataType sqlDataType = typeNames[token];
      this.SkipToken(false);
      switch (sqlDataType)
      {
        case SqlDataType.Char:
          if (this.IsToken(VARYING_TYPE_PART))
          {
            this.SkipToken(false);
            sqlDataType = SqlDataType.VarChar;
            break;
          }
          break;
        case SqlDataType.Binary:
          if (this.IsToken(VARYING_TYPE_PART))
          {
            this.SkipToken(false);
            sqlDataType = SqlDataType.VarBinary;
            break;
          }
          break;
        case SqlDataType.National:
          if (this.IsToken(TEXT_TYPE))
          {
            sqlDataType = SqlDataType.NText;
            break;
          }
          if (!this.IsToken(CHAR_TYPE))
            this.ExpectedExpression(CHARACTER_TYPE);
          if (this.SkipToken(false) && this.IsToken(VARYING_TYPE_PART))
          {
            this.SkipToken(false);
            sqlDataType = SqlDataType.NVarChar;
            break;
          }
          sqlDataType = SqlDataType.NChar;
          break;
        case SqlDataType.Double:
          this.ExpectedExpression(PRECISION_TYPE_PART);
          this.SkipToken(false);
          break;
        case SqlDataType.Table:
          this.temporaryTable = new CreateTableStatement(this.parent.Connection, this.parent, this, -1L);
          break;
      }
      return sqlDataType;
    }

    internal void SkipQuotes()
    {
      this.ExpectedExpression("(");
      this.SkipToken(true);
      uint num = 0;
      while (!this.EndOfText)
      {
        if (this.IsToken("("))
          ++num;
        else if (this.IsToken(")"))
        {
          if (num == 0U)
            break;
          --num;
        }
        this.SkipToken(true);
      }
    }

    private int ReadDataTypeLen(SqlDataType sqlDataType, out VistaDBType dataType)
    {
      this.SkipToken(true);
      bool flag = this.IsToken("MAX");
      VistaDBType vistaDbType = flag ? SQLParser.typesWithMaxLen[(int) sqlDataType] : VistaDBType.Char;
      int num1 = typesWithLen[(int) sqlDataType];
      if (num1 < 0 || vistaDbType == VistaDBType.Unknown || !flag && this.tokenValue.TokenType != TokenType.Integer)
        throw new VistaDBSQLException(624, "Expected to find DataType and Length", this.tokenValue.RowNo, this.tokenValue.ColNo);
      int num2;
      if (flag)
      {
        num2 = 0;
        dataType = vistaDbType;
      }
      else
      {
        num2 = int.Parse(this.TokenValue.Token, NumberStyles.Integer, CrossConversion.NumberFormat);
        if (num1 < num2)
          throw new VistaDBSQLException(625, num1.ToString(), this.tokenValue.RowNo, this.tokenValue.ColNo);
        dataType = sqlDataType != SqlDataType.Float || num2 > 24 ? SQLParser.sqlToNativeDataType[(int) sqlDataType] : VistaDBType.Real;
      }
      this.SkipToken(true);
      return num2;
    }

    private int ReadDataTypeScale(SqlDataType dataType)
    {
      this.SkipToken(true);
      if (typesWithScale[(int) dataType] < 0 || this.tokenValue.TokenType != TokenType.Integer)
        throw new VistaDBSQLException(624, "Expected to read DataType and Scale values", this.tokenValue.RowNo, this.tokenValue.ColNo);
      int num = int.Parse(this.TokenValue.Token, NumberStyles.Integer, CrossConversion.NumberFormat);
      this.SkipToken(true);
      return num;
    }

    public string ParseComplexName(out string objectName)
    {
      bool flag = this.IsToken("#");
      if (flag)
        this.SkipToken(true);
      string columnAndTableName = GetColumnAndTableName(this.tokenValue.Token, this.tokenValue.TokenType, this.tokenValue.RowNo, this.tokenValue.ColNo, out objectName, this.tokenValue.ValidateName);
      if (!flag)
        return TreatTemporaryTableName(columnAndTableName, this.Parent);
      if (columnAndTableName != null)
        return "#" + columnAndTableName;
      return (string) null;
    }

    public string GetTableName(Statement statement)
    {
      string token = this.tokenValue.Token;
      if (this.IsToken("#"))
      {
        this.SkipToken(true);
        token += this.tokenValue.Token;
      }
      return TreatTemporaryTableName(GetTableName(token, this.tokenValue.TokenType, this.tokenValue.RowNo, this.tokenValue.ColNo), statement);
    }

    internal static string TreatTemporaryTableName(string tableName, Statement statement)
    {
      if (tableName != null && tableName.Length > 0 && tableName[0] == '@')
      {
        CreateTableStatement temporaryTableName = statement.DoGetTemporaryTableName(tableName.Substring(1));
        if (temporaryTableName != null)
          tableName = temporaryTableName.TableName;
      }
      return tableName;
    }

    private static void InitReservedWords()
    {
      using (StringReader stringReader = new StringReader(SQLResource.ReservedWords_VDB4))
      {
        for (string key = stringReader.ReadLine(); key != null; key = stringReader.ReadLine())
          reservedWords.Add(key, 0);
      }
    }

    internal static string GetColumnAndTableName(string token, TokenType tokenType, int rowNo, int colNo, out string columnName, bool validate)
    {
      bool flag1 = validate;
      bool flag2 = validate;
      string name;
      if (tokenType == TokenType.Name)
      {
        name = (string) null;
        columnName = token;
      }
      else
      {
        int startIndex = 0;
        name = SQLParser.GetNamePart(ref startIndex, token, rowNo, colNo);
        flag1 = false;
        if (startIndex != -1)
        {
          columnName = SQLParser.GetNamePart(ref startIndex, token, rowNo, colNo);
          if (startIndex != -1)
          {
            SQLParser.CheckCorrectSchemaName(name, rowNo, colNo);
            name = columnName;
            columnName = SQLParser.GetNamePart(ref startIndex, token, rowNo, colNo);
            if (startIndex != -1)
              throw new VistaDBSQLException(608, token, rowNo, colNo);
          }
          flag2 = false;
        }
        else
        {
          columnName = name;
          name = (string) null;
        }
      }
      if (name != null)
      {
        name = name.TrimEnd();
        if (flag1)
          SQLParser.ValidateNameOrAlias(name, rowNo, colNo);
      }
      if (columnName != null)
      {
        columnName = columnName.TrimEnd();
        if (flag2)
          SQLParser.ValidateNameOrAlias(columnName, rowNo, colNo);
      }
      return name;
    }

    internal static string GetTableName(string token, TokenType tokenType, int rowNo, int colNo)
    {
      if (tokenType == TokenType.Name)
        return token;
      int startIndex = 0;
      string namePart1 = SQLParser.GetNamePart(ref startIndex, token, rowNo, colNo);
      if (startIndex == -1)
        return namePart1;
      SQLParser.CheckCorrectSchemaName(namePart1, rowNo, colNo);
      string namePart2 = SQLParser.GetNamePart(ref startIndex, token, rowNo, colNo);
      if (startIndex != -1)
        throw new VistaDBSQLException(608, token, rowNo, colNo);
      return namePart2;
    }

    internal static string GetNamePart(ref int startIndex, string name, int rowNo, int colNo)
    {
      if (name.Length <= startIndex)
        return string.Empty;
      char ch1 = name[startIndex];
      string name1;
      switch (ch1)
      {
        case '"':
        case '[':
          ++startIndex;
          char ch2 = ch1 == '"' ? ch1 : ']';
          int num1 = name.IndexOf(ch2, startIndex);
          name1 = name.Substring(startIndex, num1 - startIndex);
          startIndex = num1 + 2;
          if (startIndex > name.Length)
          {
            startIndex = -1;
            break;
          }
          break;
        default:
          int num2 = name.IndexOf(".", startIndex + 1);
          if (num2 >= 0)
          {
            name1 = name.Substring(startIndex, num2 - startIndex);
            startIndex = num2 + 1;
          }
          else
          {
            name1 = name.Substring(startIndex);
            startIndex = num2;
          }
          SQLParser.ValidateNameOrAlias(name1, rowNo, colNo);
          break;
      }
      return name1;
    }

    private static void CheckCorrectSchemaName(string name, int rowNo, int colNo)
    {
      if (string.Compare(name, "DBO", StringComparison.OrdinalIgnoreCase) != 0)
        throw new VistaDBSQLException(627, name, rowNo, colNo);
    }

    internal static void ValidateNameOrAlias(string name, int rowNo, int colNo)
    {
      if (SQLParser.reservedWords.ContainsKey(name) && !SQLParser.builtInFunctions.ContainsKey(name))
        throw new VistaDBSQLException(617, name, rowNo, colNo);
    }

    internal List<SQLParser.VariableDeclaration> ParseVariables()
    {
      if (!ParameterSignature.IsParameter(this.TokenValue.Token))
        return (List<SQLParser.VariableDeclaration>) null;
      List<SQLParser.VariableDeclaration> variableDeclarationList = new List<SQLParser.VariableDeclaration>();
      while (!this.EndOfText && ParameterSignature.IsParameter(this.TokenValue.Token))
      {
        this.CheckVariableName();
        string token = this.TokenValue.Token;
        IValue defaultValue = (IValue) null;
        Signature signature = (Signature) null;
        ParameterDirection direction = ParameterDirection.Input;
        this.SkipToken(true);
        if (this.IsToken("AS"))
          this.SkipToken(true);
        int len;
        VistaDBType dataType = this.ReadDataType(out len);
        if (dataType == VistaDBType.Unknown && this.temporaryTable != null)
        {
          this.parent.DoRegisterTemporaryTableName(token.Substring(1), this.temporaryTable);
          this.temporaryTable = (CreateTableStatement) null;
        }
        if (this.IsToken("="))
        {
          if (this.parent is DeclareStatement)
            signature = this.NextSignature(true, true, 6);
          else
            defaultValue = (IValue) this.NextSignature(true, true, 6).Execute();
        }
        if (this.IsToken("OUT"))
        {
          direction = ParameterDirection.Output;
          this.SkipToken(true);
        }
        if (this.IsToken("OUTPUT"))
        {
          direction = ParameterDirection.ReturnValue;
          this.SkipToken(true);
        }
        variableDeclarationList.Add(new SQLParser.VariableDeclaration(token, dataType, direction, defaultValue, signature));
        if (this.IsToken(","))
          this.SkipToken(true);
      }
      return variableDeclarationList;
    }

    public class TokenValueClass
    {
      private string token;
      private TokenType tokenType;
      private int rowNo;
      private int colNo;
      private int symbolNo;
      private bool validateName;

      public string Token
      {
        get
        {
          return this.token;
        }
      }

      public TokenType TokenType
      {
        get
        {
          return this.tokenType;
        }
      }

      public int RowNo
      {
        get
        {
          return this.rowNo;
        }
      }

      public int ColNo
      {
        get
        {
          return this.colNo;
        }
      }

      public int SymbolNo
      {
        get
        {
          return this.symbolNo;
        }
      }

      internal bool ValidateName
      {
        get
        {
          return !this.validateName;
        }
      }

      public void SetToken(int rowNo, int colNo, int symbolNo, string token, TokenType tokenType, bool doNotCheckAlias)
      {
        this.rowNo = rowNo;
        this.colNo = colNo;
        this.symbolNo = symbolNo;
        this.token = token;
        this.tokenType = tokenType;
        this.validateName = doNotCheckAlias;
      }

      public void SetToken(int rowNo, int colNo, int symbolNo, string token, TokenType tokenType)
      {
        this.SetToken(rowNo, colNo, symbolNo, token, tokenType, false);
      }

      public void SetPosition(int rowNo, int colNo, int symbolNo)
      {
        this.rowNo = rowNo;
        this.colNo = colNo;
        this.symbolNo = symbolNo;
      }
    }

    internal class FunctionCollection : Dictionary<string, FunctionDescr>
    {
      internal FunctionCollection()
        : base((IEqualityComparer<string>) StringComparer.OrdinalIgnoreCase)
      {
      }

      internal new FunctionDescr this[string token]
      {
        get
        {
          if (this.ContainsKey(token))
            return base[token];
          return (FunctionDescr) null;
        }
      }
    }

    internal class SpecialFunctionCollection : SQLParser.FunctionCollection
    {
    }

    internal class OperatorCollection : Dictionary<string, IOperatorDescr>
    {
      internal OperatorCollection()
        : base((IEqualityComparer<string>) StringComparer.OrdinalIgnoreCase)
      {
      }

      internal new IOperatorDescr this[string token]
      {
        get
        {
          if (this.ContainsKey(token))
            return base[token];
          return (IOperatorDescr) null;
        }
      }
    }

    internal class VariableDeclaration
    {
      public string Name;
      public VistaDBType DataType;
      public ParameterDirection Direction;
      public IValue Default;
      public Signature Signature;

      public VariableDeclaration(string origName, VistaDBType dataType)
      {
        this.Name = origName.Substring(1);
        this.DataType = dataType;
      }

      public VariableDeclaration(string origName, VistaDBType dataType, ParameterDirection direction)
        : this(origName, dataType)
      {
        this.Direction = direction;
      }

      public VariableDeclaration(string orgName, VistaDBType dataType, ParameterDirection direction, IValue defaultValue, Signature signature)
        : this(orgName, dataType, direction)
      {
        this.Default = defaultValue;
        this.Signature = signature;
      }
    }
  }
}
