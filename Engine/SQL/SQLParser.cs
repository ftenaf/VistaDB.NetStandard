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
        private static int[] typesWithLen = new int[27] { int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue, -1, -1, -1, -1, -1, -1, -1, -1, int.MaxValue, int.MaxValue, -1, -1, -1, -1, -1, -1, int.MaxValue, int.MaxValue, -1, -1, -1, -1, -1 };
        private static int[] typesWithScale = new int[26] { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, int.MaxValue, int.MaxValue, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -2 };
        private static VistaDBType[] typesWithMaxLen = new VistaDBType[27] { VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Text, VistaDBType.NText, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Image, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown, VistaDBType.Unknown };
        private static VistaDBType[] sqlToNativeDataType = new VistaDBType[27] { VistaDBType.Char, VistaDBType.NChar, VistaDBType.VarChar, VistaDBType.NVarChar, VistaDBType.Text, VistaDBType.NText, VistaDBType.TinyInt, VistaDBType.SmallInt, VistaDBType.Int, VistaDBType.BigInt, VistaDBType.Real, VistaDBType.Float, VistaDBType.Decimal, VistaDBType.Decimal, VistaDBType.Money, VistaDBType.SmallMoney, VistaDBType.Bit, VistaDBType.SmallDateTime, VistaDBType.DateTime, VistaDBType.Image, VistaDBType.VarBinary, VistaDBType.VarBinary, VistaDBType.UniqueIdentifier, VistaDBType.Unknown, VistaDBType.Timestamp, VistaDBType.Float, VistaDBType.Unknown };
        private static Dictionary<string, int> reservedWords = new Dictionary<string, int>((IEqualityComparer<string>)StringComparer.OrdinalIgnoreCase);
        private static Dictionary<string, SqlDataType> typeNames = new Dictionary<string, SqlDataType>((IEqualityComparer<string>)StringComparer.OrdinalIgnoreCase);
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
        private TokenValueClass tokenValue;
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
        private static SpecialFunctionCollection specialFunctions;
        private static FunctionCollection builtInFunctions;
        private static OperatorCollection operators;

        static SQLParser()
        {
            typeNames.Add(CHAR_TYPE, SqlDataType.Char);
            typeNames.Add(CHARACTER_TYPE, SqlDataType.Char);
            typeNames.Add(NCHAR_TYPE, SqlDataType.NChar);
            typeNames.Add(VARCHAR_TYPE, SqlDataType.VarChar);
            typeNames.Add(NVARCHAR_TYPE, SqlDataType.NVarChar);
            typeNames.Add(TEXT_TYPE, SqlDataType.Text);
            typeNames.Add(NTEXT_TYPE, SqlDataType.NText);
            typeNames.Add(TINYINT_TYPE, SqlDataType.TinyInt);
            typeNames.Add(SMALLINT_TYPE, SqlDataType.SmallInt);
            typeNames.Add(INT_TYPE, SqlDataType.Int);
            typeNames.Add(INTEGER_TYPE, SqlDataType.Int);
            typeNames.Add(BIGINT_TYPE, SqlDataType.BigInt);
            typeNames.Add(REAL_TYPE, SqlDataType.Real);
            typeNames.Add(FLOAT_TYPE, SqlDataType.Float);
            typeNames.Add(DECIMAL_TYPE, SqlDataType.Decimal);
            typeNames.Add(DEC_TYPE, SqlDataType.Decimal);
            typeNames.Add(NUMERIC_TYPE, SqlDataType.Numeric);
            typeNames.Add(MONEY_TYPE, SqlDataType.Money);
            typeNames.Add(SMALLMONEY_TYPE, SqlDataType.SmallMoney);
            typeNames.Add(BIT_TYPE, SqlDataType.Bit);
            typeNames.Add(SMALLDATETIME_TYPE, SqlDataType.SmallDateTime);
            typeNames.Add(DATETIME_TYPE, SqlDataType.DateTime);
            typeNames.Add(IMAGE_TYPE, SqlDataType.Image);
            typeNames.Add(VARBINARY_TYPE, SqlDataType.VarBinary);
            typeNames.Add(BINARY_TYPE, SqlDataType.VarBinary);
            typeNames.Add(UNIQUEIDENTIFIER_TYPE, SqlDataType.UniqueIdentifier);
            typeNames.Add(TIMESTAMP_TYPE, SqlDataType.Timestamp);
            typeNames.Add(ROWVERSION_TYPE, SqlDataType.Timestamp);
            typeNames.Add(NATIONAL_TYPE_PART, SqlDataType.National);
            typeNames.Add("TABLE", SqlDataType.Table);
            InitReservedWords();
            specialFunctions = new SpecialFunctionCollection();
            FunctionDescr functionDescr1 = (FunctionDescr)new GetViewsFunctionDescr();
            specialFunctions.Add("GETVIEWS", functionDescr1);
            specialFunctions.Add("SP_VIEWS", functionDescr1);
            FunctionDescr functionDescr2 = (FunctionDescr)new GetViewColumnsFunctionDescr();
            specialFunctions.Add("GETVIEWCOLUMNS", functionDescr2);
            specialFunctions.Add("SP_VIEWCOLUMNS", functionDescr2);
            FunctionDescr functionDescr3 = (FunctionDescr)new SpColumnsFunctionDescr();
            specialFunctions.Add("SP_COLUMNS", functionDescr3);
            FunctionDescr functionDescr4 = (FunctionDescr)new SpIndexesFunctionDescr();
            specialFunctions.Add("SP_INDEXES", functionDescr4);
            FunctionDescr functionDescr5 = (FunctionDescr)new SpStoredProceduresDescr();
            specialFunctions.Add(nameof(SP_STORED_PROCEDURES), functionDescr5);
            FunctionDescr functionDescr6 = (FunctionDescr)new SpStoredFunctionDesr();
            specialFunctions.Add("SP_UDF", functionDescr6);
            FunctionDescr functionDescr7 = (FunctionDescr)new SpForeignKeysFunctionDescr();
            specialFunctions.Add("SP_FOREIGNKEYS", functionDescr7);
            builtInFunctions = new FunctionCollection();
            foreach (string key in specialFunctions.Keys)
                builtInFunctions.Add(key, specialFunctions[key]);
            builtInFunctions.Add("LOWER", (FunctionDescr)new LowerFunctionDescr());
            builtInFunctions.Add("UPPER", (FunctionDescr)new UpperFunctionDescr());
            builtInFunctions.Add("ASCII", (FunctionDescr)new ASCIIFunctionDescr());
            builtInFunctions.Add("UNICODE", (FunctionDescr)new UnicodeFunctionDescr());
            builtInFunctions.Add("CHAR", (FunctionDescr)new CharFunctionDescr());
            builtInFunctions.Add("NCHAR", (FunctionDescr)new NCharFunctionDescr());
            builtInFunctions.Add("CHARINDEX", (FunctionDescr)new CharIndexFunctionDescr());
            builtInFunctions.Add("LEN", (FunctionDescr)new LenFunctionDescr());
            builtInFunctions.Add("LTRIM", (FunctionDescr)new LTrimFunctionDescr());
            builtInFunctions.Add("RTRIM", (FunctionDescr)new RTrimFunctionDescr());
            builtInFunctions.Add("REVERSE", (FunctionDescr)new ReverseFunctionDescr());
            builtInFunctions.Add("SPACE", (FunctionDescr)new SpaceFunctionDescr());
            builtInFunctions.Add("LEFT", (FunctionDescr)new LeftFunctionDescr());
            builtInFunctions.Add("RIGHT", (FunctionDescr)new RightFunctionDescr());
            builtInFunctions.Add("REPLACE", (FunctionDescr)new ReplaceFunctionDescr());
            builtInFunctions.Add("REPLICATE", (FunctionDescr)new ReplicateFunctionDescr());
            builtInFunctions.Add("STR", (FunctionDescr)new StrFunctionDescr());
            builtInFunctions.Add("STUFF", (FunctionDescr)new StuffFunctionDescr());
            builtInFunctions.Add("SUBSTRING", (FunctionDescr)new SubStringFunctionDescr());
            builtInFunctions.Add("PATINDEX", (FunctionDescr)new PAtIndexFunctionDescr());
            builtInFunctions.Add("ABS", (FunctionDescr)new AbsFunctionDescr());
            builtInFunctions.Add("ACOS", (FunctionDescr)new ACosFunctionDescr());
            builtInFunctions.Add("ASIN", (FunctionDescr)new ASinFunctionDescr());
            builtInFunctions.Add("ATAN", (FunctionDescr)new ATanFunctionDescr());
            builtInFunctions.Add("ATN2", (FunctionDescr)new ATN2FunctionDescr());
            builtInFunctions.Add("CEILING", (FunctionDescr)new CeilingFunctionDescr());
            builtInFunctions.Add("COS", (FunctionDescr)new CosFunctionDescr());
            builtInFunctions.Add("COT", (FunctionDescr)new CotFunctionDescr());
            builtInFunctions.Add("DEGREES", (FunctionDescr)new DegreesFunctionDescr());
            builtInFunctions.Add("EXP", (FunctionDescr)new ExpFunctionDescr());
            builtInFunctions.Add("FLOOR", (FunctionDescr)new FloorFunctionDescr());
            builtInFunctions.Add("FRAC", (FunctionDescr)new FracFunctionDescr());
            builtInFunctions.Add("INT", (FunctionDescr)new IntFunctionDescr());
            builtInFunctions.Add("LOG", (FunctionDescr)new LogFunctionDescr());
            builtInFunctions.Add("LOG10", (FunctionDescr)new Log10FunctionDescr());
            builtInFunctions.Add("MAXOF", (FunctionDescr)new MaxOfFunctionDescr());
            builtInFunctions.Add("MINOF", (FunctionDescr)new MinOfFunctionDescr());
            builtInFunctions.Add("PI", (FunctionDescr)new PIFunctionDescr());
            builtInFunctions.Add("POWER", (FunctionDescr)new PowerFunctionDescr());
            builtInFunctions.Add("RADIANS", (FunctionDescr)new RadiansFunctionDescr());
            builtInFunctions.Add("RAND", (FunctionDescr)new RandFunctionDescr());
            builtInFunctions.Add("ROUND", (FunctionDescr)new RoundFunctionDescr());
            builtInFunctions.Add("SIGN", (FunctionDescr)new SignFunctionDescr());
            builtInFunctions.Add("SIN", (FunctionDescr)new SinFunctionDescr());
            builtInFunctions.Add("SQRT", (FunctionDescr)new SqrtFunctionDescr());
            builtInFunctions.Add("SQUARE", (FunctionDescr)new SquareFunctionDescr());
            builtInFunctions.Add("TAN", (FunctionDescr)new TanFunctionDescr());
            builtInFunctions.Add("SUM", (FunctionDescr)new SumFunctionDescr());
            builtInFunctions.Add("COUNT", (FunctionDescr)new CountFunctionDescr());
            builtInFunctions.Add("COUNT_BIG", (FunctionDescr)new CountBigFunctionDescr());
            builtInFunctions.Add("AVG", (FunctionDescr)new AvgFunctionDescr());
            builtInFunctions.Add("MIN", (FunctionDescr)new MinFunctionDescr());
            builtInFunctions.Add("MAX", (FunctionDescr)new MaxFunctionDescr());
            builtInFunctions.Add("STDEV", (FunctionDescr)new StDevFunctionDescr());
            builtInFunctions.Add("CAST", (FunctionDescr)new CastFunctionDescr());
            builtInFunctions.Add("ISNULL", (FunctionDescr)new IsNullFunctionDescr());
            builtInFunctions.Add("LOOKUP", (FunctionDescr)new LookupFunctionDescr());
            builtInFunctions.Add("NULLIF", (FunctionDescr)new NullIfFunctionDescr());
            builtInFunctions.Add("ISNUMERIC", (FunctionDescr)new IsNumericFunctionDescr());
            builtInFunctions.Add("CONVERT", (FunctionDescr)new ConvertFunctionDescr());
            builtInFunctions.Add("CASE", (FunctionDescr)new CaseFunctionDescr());
            builtInFunctions.Add("LASTIDENTITY", (FunctionDescr)new LastIdentityFunctionDescr());
            builtInFunctions.Add("CONTAINS", (FunctionDescr)new ContainsFunctionDescr());
            builtInFunctions.Add("COALESCE", (FunctionDescr)new CoalesceFunctionDescr());
            builtInFunctions.Add("NEWID", (FunctionDescr)new NewIDFunctionDescr());
            builtInFunctions.Add("SP_RENAME", (FunctionDescr)new RenameFunctionDescr());
            builtInFunctions.Add("IIF", (FunctionDescr)new IIFFunctionDescr());
            builtInFunctions.Add("LASTTIMESTAMP", (FunctionDescr)new LastTimestampFunctionDescr());
            builtInFunctions.Add(nameof(LASTTABLEANCHOR), (FunctionDescr)new LastTableAnchorDesc());
            builtInFunctions.Add("DATEADD", (FunctionDescr)new DateAddFunctionDescr());
            builtInFunctions.Add("DATEDIFF", (FunctionDescr)new DateDiffFunctionDescr());
            builtInFunctions.Add("DATENAME", (FunctionDescr)new DateNameFunctionDescr());
            builtInFunctions.Add("DATEPART", (FunctionDescr)new DatePartFunctionDescr());
            builtInFunctions.Add("DAY", (FunctionDescr)new DayFunctionDescr());
            builtInFunctions.Add("GETDATE", (FunctionDescr)new GetDateFunctionDescr());
            builtInFunctions.Add("GETUTCDATE", (FunctionDescr)new GetUtcDateFunctionDescr());
            builtInFunctions.Add("MONTH", (FunctionDescr)new MonthFunctionDescr());
            builtInFunctions.Add("YEAR", (FunctionDescr)new YearFunctionDescr());
            builtInFunctions.Add("@@IDENTITY", (FunctionDescr)new IdentityVariableDescr());
            builtInFunctions.Add("@@VERSION", (FunctionDescr)new VistaDBVersionDescr());
            builtInFunctions.Add("@@ERROR", (FunctionDescr)new VistaDBErrorVariableDescription());
            builtInFunctions.Add("@@DATABASEID", (FunctionDescr)new VistaDBDatabaseIdVariableDescriptor());
            builtInFunctions.Add("@@ROWCOUNT", (FunctionDescr)new VistaDBRowCountVariableDescription());
            builtInFunctions.Add("@@TRANCOUNT", (FunctionDescr)new VistaDBTranCountVariableDescription());
            operators = new OperatorCollection();
            operators.Add("EXISTS UNARY", (IOperatorDescr)new ExistsOperatorDescr());
            operators.Add("~ UNARY", (IOperatorDescr)new BitwiseNotOperatorDescr());
            operators.Add("*", (IOperatorDescr)new MultiplyOperatorDescr());
            operators.Add("/", (IOperatorDescr)new DivideOperatorDescr());
            operators.Add("%", (IOperatorDescr)new ModOperatorDescr());
            operators.Add("- UNARY", (IOperatorDescr)new UnaryMinusOperatorDescr());
            operators.Add("+ UNARY", (IOperatorDescr)new UnaryPlusOperatorDescr());
            operators.Add("+", (IOperatorDescr)new PlusOperatorDescr());
            operators.Add("-", (IOperatorDescr)new MinusOperatorDescr());
            operators.Add("&", (IOperatorDescr)new BitwiseAndOperatorDescr());
            operators.Add("|", (IOperatorDescr)new BitwiseOrOperatorDescr());
            operators.Add("^", (IOperatorDescr)new BitwiseXorOperatorDescr());
            operators.Add("=", (IOperatorDescr)new EqualOperatorDescr());
            IOperatorDescr operatorDescr = (IOperatorDescr)new NotEqualOperatorDescr();
            operators.Add("!=", operatorDescr);
            operators.Add("<>", operatorDescr);
            operators.Add("<", (IOperatorDescr)new LessThanOperatorDescr());
            operators.Add("<=", (IOperatorDescr)new LessOrEqualOperatorDescr());
            operators.Add(">", (IOperatorDescr)new GreaterThanOperatorDescr());
            operators.Add(">=", (IOperatorDescr)new GreaterOrEqualOperatorDescr());
            operators.Add("IN", (IOperatorDescr)new InOperatorDescr());
            operators.Add("LIKE", (IOperatorDescr)new LikeOperatorDescr());
            operators.Add("BETWEEN", (IOperatorDescr)new BetweenOperatorDescr());
            operators.Add("IS", (IOperatorDescr)new IsNullOperatorDescr());
            operators.Add("NOT", (IOperatorDescr)new NotBaseOperatorDescr());
            operators.Add("NOT UNARY", (IOperatorDescr)new NotOperatorDescr());
            operators.Add("AND", (IOperatorDescr)new AndOperatorDescr());
            operators.Add("OR", (IOperatorDescr)new OrOperatorDescr());
        }

        internal static SQLParser CreateInstance(string text, CultureInfo culture)
        {
            return new SQLParser(text, culture);
        }

        private SQLParser(string text, CultureInfo culture)
        {
            parent = (Statement)null;
            tokenValue = new TokenValueClass();
            currentContext = new Stack();
            this.culture = culture;
            SetText(text);
        }

        public void SetText(string text)
        {
            this.text = text;
            textLength = this.text.Length;
            symbolNo = 0;
            rowNo = 1;
            colNo = 1;
        }

        public Statement Parent
        {
            get
            {
                return parent;
            }
            set
            {
                parent = value;
            }
        }

        public string Text
        {
            get
            {
                return text;
            }
        }

        public int SymbolNo
        {
            get
            {
                return symbolNo;
            }
        }

        public bool EndOfText
        {
            get
            {
                return tokenValue.Token == null;
            }
        }

        public CultureInfo Culture
        {
            get
            {
                return culture;
            }
            set
            {
                culture = value;
            }
        }

        internal CurrentTokenContext Context
        {
            get
            {
                return currentContext.Peek() as CurrentTokenContext;
            }
        }

        internal void PushContext(CurrentTokenContext context)
        {
            currentContext.Push((object)context);
        }

        internal void PopContext()
        {
            currentContext.Pop();
        }

        private bool CheckUdfContext(string udfName)
        {
            foreach (CurrentTokenContext currentTokenContext in currentContext)
            {
                if (currentTokenContext.ContextType == CurrentTokenContext.TokenContext.StoredFunction)
                    return currentTokenContext.ContextName.CompareTo(udfName) == 0;
            }
            return false;
        }

        internal Signature NextSignature(bool needSkip, bool raiseException, int priority)
        {
            if (needSkip && !SkipToken(raiseException))
                return (Signature)null;
            Signature signature = priority == -1 ? ParseExpressions() : ParsePriority(priority);
            if (raiseException && signature == (Signature)null)
                throw new VistaDBSQLException(502, "end of text", rowNo, symbolNo + 1);
            return signature;
        }

        internal TokenValueClass TokenValue
        {
            get
            {
                return tokenValue;
            }
        }

        internal void ExpectedExpression(string expression, params string[] alternative)
        {
            if (tokenValue.TokenType == TokenType.String || string.Compare(expression, tokenValue.Token, StringComparison.OrdinalIgnoreCase) != 0)
            {
                if (alternative != null)
                {
                    foreach (string str in alternative)
                    {
                        expression += ", ";
                        expression += str;
                    }
                }
                throw new VistaDBSQLException(507, expression, tokenValue.RowNo, tokenValue.ColNo);
            }
        }

        internal bool IsToken(string expression)
        {
            if (tokenValue.TokenType != TokenType.String)
                return string.Compare(tokenValue.Token, expression, StringComparison.OrdinalIgnoreCase) == 0;
            return false;
        }

        internal bool TokenEndsWith(string expression)
        {
            if (tokenValue.TokenType != TokenType.String)
                return tokenValue.Token.EndsWith(expression, StringComparison.OrdinalIgnoreCase);
            return false;
        }

        internal bool TokenIsSystemFunction()
        {
            return builtInFunctions.ContainsKey(TokenValue.Token);
        }

        internal ITableValuedFunction CreateSpecialFunction(string name, int rowNo, int colNo, int symbolNo)
        {
            tokenValue.SetPosition(rowNo, colNo, symbolNo);
            FunctionDescr specialFunction = specialFunctions[name];
            if (specialFunction != null)
                return (ITableValuedFunction)specialFunction.CreateSignature(this);
            IUserDefinedFunctionInformation userDefinedFunction = parent.Database.GetUserDefinedFunctions()[name];
            if (userDefinedFunction == null)
                return (ITableValuedFunction)new CLRResultSetFunction(this, name);
            if (userDefinedFunction.ScalarValued)
                throw new Exception("Scalar-valued function can't be executed from exec");
            return (ITableValuedFunction)new TableValuedFunction(this, userDefinedFunction);
        }

        internal void CheckVariableName()
        {
            string token = TokenValue.Token;
            if (token[0] != '@')
                throw new VistaDBSQLException(500, "@", TokenValue.RowNo, TokenValue.ColNo);
            if (token.Length == 1)
                throw new VistaDBSQLException(621, token, TokenValue.RowNo, TokenValue.ColNo);
        }

        internal bool SkipSemicolons()
        {
            bool endOfText;
            for (endOfText = EndOfText; !endOfText && IsToken(";"); endOfText = EndOfText)
                SkipToken(false);
            return endOfText;
        }

        public void SuppressNextSkipToken()
        {
            suppressSkip = true;
        }

        public bool SkipToken(bool raiseException)
        {
            int symbolNo1 = symbolNo;
            if (suppressSkip)
            {
                suppressSkip = false;
                if (tokenValue.Token != null)
                    return true;
                if (raiseException)
                    throw new VistaDBSQLException(502, "end of text", this.rowNo, symbolNo + 1);
                return false;
            }

            while (SkipComments()) ;

            if (textLength == symbolNo)
            {
                tokenValue.SetToken(0, 0, 0, (string)null, TokenType.Unknown);
                if (raiseException)
                    throw new VistaDBSQLException(502, "end of text", this.rowNo, symbolNo + 1);
                return false;
            }
            char c = text[symbolNo];
            int symbolNo2 = symbolNo;
            int rowNo = this.rowNo;
            int colNo = this.colNo;
            tokenLen = 0;
            bool doNotCheckAlias = false;
            TokenType tokenType;
            if (c == '0' && textLength > symbolNo + 1 && (text[symbolNo + 1] == 'x' || text[symbolNo + 1] == 'X'))
                tokenType = ReadBinary();
            else if (IsNumeric(c) || c == '.' && textLength > symbolNo + 1 && IsNumeric(text[symbolNo + 1]))
                tokenType = ReadNumeric(c);
            else if (IsLetter(c))
            {
                tokenType = ReadUnknown(c, ref doNotCheckAlias);
            }
            else
            {
                switch (c)
                {
                    case '"':
                    case '[':
                        tokenType = ReadName(c, ref doNotCheckAlias);
                        break;
                    case '\'':
                        tokenType = ReadString(c);
                        break;
                    default:
                        tokenType = ReadSpecialSymbol(c);
                        break;
                }
            }
            string token;
            switch (tokenType)
            {
                case TokenType.String:
                    token = text.Substring(symbolNo2 + 1, tokenLen - 2).Replace("''", '\''.ToString());
                    break;
                case TokenType.Name:
                    token = text.Substring(symbolNo2 + 1, tokenLen - 2);
                    break;
                default:
                    token = text.Substring(symbolNo2, tokenLen);
                    break;
            }
            tokenValue.SetToken(rowNo, colNo, symbolNo1, token, tokenType, doNotCheckAlias);
            return true;
        }

        public string GetStringUntil(char stopChar)
        {
            while (SkipComments());
            if (text[this.symbolNo] == '\'')
            {
                if (!SkipToken(false))
                    return (string)null;
                return tokenValue.Token;
            }
            int symbolNo = this.symbolNo;
            while (textLength > this.symbolNo && (int)text[this.symbolNo] != (int)stopChar)
                IncrementSymbolNo(true);
            if (symbolNo == this.symbolNo)
                return (string)null;
            return text.Substring(symbolNo, this.symbolNo - symbolNo);
        }

        private TokenType ReadBinary()
        {
            IncrementSymbolNo(false);
            while (IncrementSymbolNo(false) && IsHex(text[symbolNo]));
            return TokenType.Binary;
        }

        private TokenType ReadString(char c)
        {
            bool flag = false;
            if (IncrementSymbolNo(true) || textLength != symbolNo)
            {
                for (c = text[symbolNo]; c != '\'' || IncrementSymbolNo(false) && text[symbolNo] == '\''; c = text[symbolNo])
                {
                    if (!IncrementSymbolNo(true) && textLength == symbolNo)
                        goto label_6;
                }
                flag = true;
            }
        label_6:
            if (!flag)
                throw new VistaDBSQLException(503, "", rowNo, colNo + 1);
            return TokenType.String;
        }

        private TokenType ReadNumeric(char c)
        {
            TokenType tokenType = c == '.' ? TokenType.Float : TokenType.Integer;
            if (IncrementSymbolNo(false))
            {
                for (c = text[symbolNo]; IsNumericExt(c); c = text[symbolNo])
                {
                    switch (c)
                    {
                        case '+':
                        case '-':
                            c = text[symbolNo - 1];
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
                    if (!IncrementSymbolNo(false))
                        break;
                }
            }
        label_9:
            return tokenType;
        }

        private TokenType ReadUnknown(char c, ref bool doNotCheckAlias)
        {
            if (IncrementSymbolNo(false))
            {
                char ch = char.MinValue;
                c = text[symbolNo];
                while ((IsLetter(c) || IsNumeric(c) || ch == '.') && IncrementSymbolNo(false))
                {
                    ch = c;
                    c = text[symbolNo];
                    if (ch == '.' && (c == '[' || c == '"'))
                    {
                        int num = (int)ReadName(c, ref doNotCheckAlias);
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
                        if (textLength > symbolNo + 1 && text[symbolNo + 1] == '=')
                        {
                            IncrementSymbolNo(false);
                            break;
                        }
                        break;
                    case '<':
                        if (textLength > symbolNo + 1 && (text[symbolNo + 1] == '>' || text[symbolNo + 1] == '='))
                        {
                            IncrementSymbolNo(false);
                            break;
                        }
                        break;
                    case '>':
                        if (textLength > symbolNo + 1 && text[symbolNo + 1] == '=')
                        {
                            IncrementSymbolNo(false);
                            break;
                        }
                        break;
                    case '|':
                        if (textLength > symbolNo + 1 && text[symbolNo + 1] == '|')
                        {
                            IncrementSymbolNo(false);
                            break;
                        }
                        break;
                }
                tokenType = TokenType.Unknown;
            }
            IncrementSymbolNo(false);
            return tokenType;
        }

        private TokenType ReadName(char c, ref bool doNotCheckAlias)
        {
            char ch1 = c == '"' ? c : ']';
            while (IncrementSymbolNo(false))
            {
                c = text[symbolNo];
                if ((int)c == (int)ch1)
                {
                    doNotCheckAlias = true;
                    if (!IncrementSymbolNo(false) || text[symbolNo] != '.')
                        return TokenType.Name;
                    if (!IncrementSymbolNo(false))
                        throw new VistaDBSQLException(505, "", rowNo, colNo + 1);
                    c = text[symbolNo];
                    if (c == '[' || c == '"')
                    {
                        char ch2 = c == '"' ? c : ']';
                        while (IncrementSymbolNo(false))
                        {
                            c = text[symbolNo];
                            if ((int)c == (int)ch2)
                            {
                                IncrementSymbolNo(false);
                                goto label_15;
                            }
                        }
                        throw new VistaDBSQLException(505, "", rowNo, colNo + 1);
                    }
                    int num = (int)ReadUnknown(c, ref doNotCheckAlias);
                label_15:
                    return TokenType.ComplexName;
                }
            }
            throw new VistaDBSQLException(504, "", rowNo, colNo + 1);
        }

        private bool SkipComments()
        {
            SkipSpaces();
            if (textLength <= symbolNo + 2 - 1)
                return false;
            string strA = text.Substring(symbolNo, 2);
            if (string.Compare(strA, "/*", StringComparison.OrdinalIgnoreCase) == 0)
            {
                symbolNo += 2;
                while (textLength > symbolNo + "*/".Length - 1)
                {
                    if (string.Compare(text.Substring(symbolNo, "/*".Length), "*/", StringComparison.OrdinalIgnoreCase) == 0)
                    {
                        symbolNo += "*/".Length;
                        SkipSpaces();
                        return true;
                    }
                    IncrementSymbolNo(false);
                }
                throw new VistaDBSQLException(506, "", rowNo, colNo + 1);
            }
            if (string.Compare(strA, "--", StringComparison.OrdinalIgnoreCase) == 0)
            {
                symbolNo += 2;
                for (; textLength > symbolNo; ++symbolNo)
                {
                    switch (text[symbolNo])
                    {
                        case '\n':
                        case '\r':
                            SkipSpaces();
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
            while (textLength > symbolNo && (text[symbolNo] == '\t' || text[symbolNo] <= ' '))
                IncrementSymbolNo(false);
        }

        private bool IncrementSymbolNo(bool addNewLineLen)
        {
            ++symbolNo;
            ++tokenLen;
            if (textLength == symbolNo)
                return false;
            switch (text[symbolNo])
            {
                case '\t':
                    colNo += 4 - colNo % 4;
                    return false;
                case '\n':
                    ++symbolNo;
                    if (addNewLineLen)
                        ++tokenLen;
                    ++rowNo;
                    colNo = 1;
                    return false;
                case '\r':
                    ++symbolNo;
                    if (addNewLineLen)
                        ++tokenLen;
                    ++rowNo;
                    colNo = 1;
                    if (textLength > symbolNo && text[symbolNo] == '\n')
                    {
                        ++symbolNo;
                        if (addNewLineLen)
                            ++tokenLen;
                    }
                    return false;
                default:
                    ++colNo;
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
            if (tokenValue.TokenType != TokenType.Unknown)
            {
                leftSignature = ParseExpressions();
                index = tokenValue.Token;
            }
            else
            {
                leftSignature = (Signature)null;
                index = tokenValue.Token + " UNARY";
            }
            int num = 0;
            while (tokenValue.TokenType == TokenType.Unknown && !EndOfText)
            {
                IOperatorDescr operatorDescr = operators[index];
                if (operatorDescr == null || operatorDescr.Priority < num || operatorDescr.Priority > priority)
                {
                    if (leftSignature != (Signature)null)
                        return leftSignature;
                    leftSignature = ParseExpressions();
                }
                else
                {
                    leftSignature = operatorDescr.CreateSignature(leftSignature, this);
                    num = operatorDescr.Priority;
                }
                index = tokenValue.Token;
            }
            return leftSignature;
        }

        private Signature ParseExpressions()
        {
            bool flag = true;
            Signature signature = (Signature)ConstantSignature.CreateSignature(this);
            if (signature == (Signature)null)
            {
                switch (tokenValue.TokenType)
                {
                    case TokenType.Unknown:
                        string token = tokenValue.Token;
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
                        signature = ParseFunctions(true);
                        if (signature == (Signature)null)
                        {
                            signature = (Signature)ColumnSignature.CreateSignature(this);
                            break;
                        }
                        break;
                    case TokenType.LeftBracket:
                        SkipToken(true);
                        signature = ParsePriority(6);
                        ExpectedExpression(")");
                        break;
                    case TokenType.Name:
                    case TokenType.ComplexName:
                        signature = ParseFunctions(false);
                        if (!(signature != (Signature)null))
                        {
                            signature = (Signature)ColumnSignature.CreateSignature(this);
                            break;
                        }
                        break;
                    default:
                        signature = (Signature)null;
                        break;
                }
            }
            if (flag)
                SkipToken(false);
            return signature;
        }

        private Signature ParseFunctions(bool includeSystem)
        {
            string token = tokenValue.Token;
            FunctionDescr functionDescr = includeSystem ? builtInFunctions[token] : (FunctionDescr)null;
            int symbolNo1 = symbolNo;
            int rowNo1 = rowNo;
            int colNo1 = colNo;
            int symbolNo2 = tokenValue.SymbolNo;
            int rowNo2 = tokenValue.RowNo;
            int colNo2 = tokenValue.ColNo;
            bool validateName = tokenValue.ValidateName;
            TokenType tokenType = tokenValue.TokenType;
            SkipToken(false);
            if (parent.Connection.Database != null)
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
                if (IsToken("("))
                {
                    IUserDefinedFunctionCollection definedFunctions = parent.Connection.Database.GetUserDefinedFunctions();
                    if (definedFunctions.ContainsKey(index))
                        functionDescr = (FunctionDescr)new StoredFunctionDescr(definedFunctions[index]);
                    else if (CheckUdfContext(index))
                    {
                        SkipQuotes();
                        return NextSignature(true, true, 6);
                    }
                }
                else
                {
                    IStoredProcedureCollection storedProcedures = parent.Connection.Database.GetStoredProcedures();
                    if (storedProcedures.ContainsKey(index))
                        functionDescr = (FunctionDescr)new StoredProcedureDescr(storedProcedures[index]);
                }
                if (parent.Connection.Database.TryGetProcedure(index, out ClrHosting.ClrProcedure procedure))
                    return (Signature)new CLRStoredProcedure(this, index);
                if (parent.Connection.Database.GetClrProcedures().ContainsKey(index))
                    return (Signature)new CLRStoredProcedure(this, index);
            }
            SetPosition(rowNo1, colNo1, symbolNo1, rowNo2, colNo2, symbolNo2, token, tokenType, !validateName);
            return functionDescr?.CreateSignature(this);
        }

        private void SetPosition(int rowNo, int colNo, int symbolNo, int tokenRowNo, int tokenColNo, int tokenSymbolNo, string token, TokenType tokenType, bool doNotCheckAlias)
        {
            this.rowNo = rowNo;
            this.colNo = colNo;
            this.symbolNo = symbolNo;
            tokenValue.SetToken(tokenRowNo, tokenColNo, tokenSymbolNo, token, tokenType, doNotCheckAlias);
        }

        public VistaDBType ReadDataType(out int len)
        {
            SqlDataType sqlDataType = ReadSqlDataType();
            len = 0;
            if (!IsToken("("))
                return sqlToNativeDataType[(int)sqlDataType];
            VistaDBType dataType;
            len = ReadDataTypeLen(sqlDataType, out dataType);
            if (IsToken(")"))
            {
                SkipToken(false);
                return dataType;
            }
            ExpectedExpression(",");
            ReadDataTypeScale(sqlDataType);
            ExpectedExpression(")");
            SkipToken(false);
            return dataType;
        }

        private SqlDataType ReadSqlDataType()
        {
            temporaryTable = (CreateTableStatement)null;
            string token = tokenValue.Token;
            if (!typeNames.ContainsKey(token))
                throw new VistaDBSQLException(624, "Expected to find Sql Data Type", tokenValue.RowNo, tokenValue.ColNo);
            SqlDataType sqlDataType = typeNames[token];
            SkipToken(false);
            switch (sqlDataType)
            {
                case SqlDataType.Char:
                    if (IsToken(VARYING_TYPE_PART))
                    {
                        SkipToken(false);
                        sqlDataType = SqlDataType.VarChar;
                        break;
                    }
                    break;
                case SqlDataType.Binary:
                    if (IsToken(VARYING_TYPE_PART))
                    {
                        SkipToken(false);
                        sqlDataType = SqlDataType.VarBinary;
                        break;
                    }
                    break;
                case SqlDataType.National:
                    if (IsToken(TEXT_TYPE))
                    {
                        sqlDataType = SqlDataType.NText;
                        break;
                    }
                    if (!IsToken(CHAR_TYPE))
                        ExpectedExpression(CHARACTER_TYPE);
                    if (SkipToken(false) && IsToken(VARYING_TYPE_PART))
                    {
                        SkipToken(false);
                        sqlDataType = SqlDataType.NVarChar;
                        break;
                    }
                    sqlDataType = SqlDataType.NChar;
                    break;
                case SqlDataType.Double:
                    ExpectedExpression(PRECISION_TYPE_PART);
                    SkipToken(false);
                    break;
                case SqlDataType.Table:
                    temporaryTable = new CreateTableStatement(parent.Connection, parent, this, -1L);
                    break;
            }
            return sqlDataType;
        }

        internal void SkipQuotes()
        {
            ExpectedExpression("(");
            SkipToken(true);
            uint num = 0;
            while (!EndOfText)
            {
                if (IsToken("("))
                    ++num;
                else if (IsToken(")"))
                {
                    if (num == 0U)
                        break;
                    --num;
                }
                SkipToken(true);
            }
        }

        private int ReadDataTypeLen(SqlDataType sqlDataType, out VistaDBType dataType)
        {
            SkipToken(true);
            bool flag = IsToken("MAX");
            VistaDBType vistaDbType = flag ? typesWithMaxLen[(int)sqlDataType] : VistaDBType.Char;
            int num1 = typesWithLen[(int)sqlDataType];
            if (num1 < 0 || vistaDbType == VistaDBType.Unknown || !flag && tokenValue.TokenType != TokenType.Integer)
                throw new VistaDBSQLException(624, "Expected to find DataType and Length", tokenValue.RowNo, tokenValue.ColNo);
            int num2;
            if (flag)
            {
                num2 = 0;
                dataType = vistaDbType;
            }
            else
            {
                num2 = int.Parse(TokenValue.Token, NumberStyles.Integer, CrossConversion.NumberFormat);
                if (num1 < num2)
                    throw new VistaDBSQLException(625, num1.ToString(), tokenValue.RowNo, tokenValue.ColNo);
                dataType = sqlDataType != SqlDataType.Float || num2 > 24 ? sqlToNativeDataType[(int)sqlDataType] : VistaDBType.Real;
            }
            SkipToken(true);
            return num2;
        }

        private int ReadDataTypeScale(SqlDataType dataType)
        {
            SkipToken(true);
            if (typesWithScale[(int)dataType] < 0 || tokenValue.TokenType != TokenType.Integer)
                throw new VistaDBSQLException(624, "Expected to read DataType and Scale values", tokenValue.RowNo, tokenValue.ColNo);
            int num = int.Parse(TokenValue.Token, NumberStyles.Integer, CrossConversion.NumberFormat);
            SkipToken(true);
            return num;
        }

        public string ParseComplexName(out string objectName)
        {
            bool flag = IsToken("#");
            if (flag)
                SkipToken(true);
            string columnAndTableName = GetColumnAndTableName(tokenValue.Token, tokenValue.TokenType, tokenValue.RowNo, tokenValue.ColNo, out objectName, tokenValue.ValidateName);
            if (!flag)
                return TreatTemporaryTableName(columnAndTableName, Parent);
            if (columnAndTableName != null)
                return "#" + columnAndTableName;
            return (string)null;
        }

        public string GetTableName(Statement statement)
        {
            string token = tokenValue.Token;
            if (IsToken("#"))
            {
                SkipToken(true);
                token += tokenValue.Token;
            }
            return TreatTemporaryTableName(GetTableName(token, tokenValue.TokenType, tokenValue.RowNo, tokenValue.ColNo), statement);
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
                name = (string)null;
                columnName = token;
            }
            else
            {
                int startIndex = 0;
                name = GetNamePart(ref startIndex, token, rowNo, colNo);
                flag1 = false;
                if (startIndex != -1)
                {
                    columnName = GetNamePart(ref startIndex, token, rowNo, colNo);
                    if (startIndex != -1)
                    {
                        CheckCorrectSchemaName(name, rowNo, colNo);
                        name = columnName;
                        columnName = GetNamePart(ref startIndex, token, rowNo, colNo);
                        if (startIndex != -1)
                            throw new VistaDBSQLException(608, token, rowNo, colNo);
                    }
                    flag2 = false;
                }
                else
                {
                    columnName = name;
                    name = (string)null;
                }
            }
            if (name != null)
            {
                name = name.TrimEnd();
                if (flag1)
                    ValidateNameOrAlias(name, rowNo, colNo);
            }
            if (columnName != null)
            {
                columnName = columnName.TrimEnd();
                if (flag2)
                    ValidateNameOrAlias(columnName, rowNo, colNo);
            }
            return name;
        }

        internal static string GetTableName(string token, TokenType tokenType, int rowNo, int colNo)
        {
            if (tokenType == TokenType.Name)
                return token;
            int startIndex = 0;
            string namePart1 = GetNamePart(ref startIndex, token, rowNo, colNo);
            if (startIndex == -1)
                return namePart1;
            CheckCorrectSchemaName(namePart1, rowNo, colNo);
            string namePart2 = GetNamePart(ref startIndex, token, rowNo, colNo);
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
                    ValidateNameOrAlias(name1, rowNo, colNo);
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
            if (reservedWords.ContainsKey(name) && !builtInFunctions.ContainsKey(name))
                throw new VistaDBSQLException(617, name, rowNo, colNo);
        }

        internal List<VariableDeclaration> ParseVariables()
        {
            if (!ParameterSignature.IsParameter(TokenValue.Token))
                return (List<VariableDeclaration>)null;
            List<VariableDeclaration> variableDeclarationList = new List<VariableDeclaration>();
            while (!EndOfText && ParameterSignature.IsParameter(TokenValue.Token))
            {
                CheckVariableName();
                string token = TokenValue.Token;
                IValue defaultValue = (IValue)null;
                Signature signature = (Signature)null;
                ParameterDirection direction = ParameterDirection.Input;
                SkipToken(true);
                if (IsToken("AS"))
                    SkipToken(true);
                int len;
                VistaDBType dataType = ReadDataType(out len);
                if (dataType == VistaDBType.Unknown && temporaryTable != null)
                {
                    parent.DoRegisterTemporaryTableName(token.Substring(1), temporaryTable);
                    temporaryTable = (CreateTableStatement)null;
                }
                if (IsToken("="))
                {
                    if (parent is DeclareStatement)
                        signature = NextSignature(true, true, 6);
                    else
                        defaultValue = (IValue)NextSignature(true, true, 6).Execute();
                }
                if (IsToken("OUT"))
                {
                    direction = ParameterDirection.Output;
                    SkipToken(true);
                }
                if (IsToken("OUTPUT"))
                {
                    direction = ParameterDirection.ReturnValue;
                    SkipToken(true);
                }
                variableDeclarationList.Add(new VariableDeclaration(token, dataType, direction, defaultValue, signature));
                if (IsToken(","))
                    SkipToken(true);
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
                    return token;
                }
            }

            public TokenType TokenType
            {
                get
                {
                    return tokenType;
                }
            }

            public int RowNo
            {
                get
                {
                    return rowNo;
                }
            }

            public int ColNo
            {
                get
                {
                    return colNo;
                }
            }

            public int SymbolNo
            {
                get
                {
                    return symbolNo;
                }
            }

            internal bool ValidateName
            {
                get
                {
                    return !validateName;
                }
            }

            public void SetToken(int rowNo, int colNo, int symbolNo, string token, TokenType tokenType, bool doNotCheckAlias)
            {
                this.rowNo = rowNo;
                this.colNo = colNo;
                this.symbolNo = symbolNo;
                this.token = token;
                this.tokenType = tokenType;
                validateName = doNotCheckAlias;
            }

            public void SetToken(int rowNo, int colNo, int symbolNo, string token, TokenType tokenType)
            {
                SetToken(rowNo, colNo, symbolNo, token, tokenType, false);
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
              : base((IEqualityComparer<string>)StringComparer.OrdinalIgnoreCase)
            {
            }

            internal new FunctionDescr this[string token]
            {
                get
                {
                    if (ContainsKey(token))
                        return base[token];
                    return (FunctionDescr)null;
                }
            }
        }

        internal class SpecialFunctionCollection : FunctionCollection
        {
        }

        internal class OperatorCollection : Dictionary<string, IOperatorDescr>
        {
            internal OperatorCollection()
              : base((IEqualityComparer<string>)StringComparer.OrdinalIgnoreCase)
            {
            }

            internal new IOperatorDescr this[string token]
            {
                get
                {
                    if (ContainsKey(token))
                        return base[token];
                    return (IOperatorDescr)null;
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
                Name = origName.Substring(1);
                DataType = dataType;
            }

            public VariableDeclaration(string origName, VistaDBType dataType, ParameterDirection direction)
              : this(origName, dataType)
            {
                Direction = direction;
            }

            public VariableDeclaration(string orgName, VistaDBType dataType, ParameterDirection direction, IValue defaultValue, Signature signature)
              : this(orgName, dataType, direction)
            {
                Default = defaultValue;
                Signature = signature;
            }
        }
    }
}
