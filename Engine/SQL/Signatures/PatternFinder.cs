using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core.Indexing;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class PatternFinder
  {
    private string pattern;
    private string escapeCharacter;
    private List<Chunk> chunks;
    private bool alwaysOne;
    private int lineNo;
    private int symbolNo;
    private CultureInfo connCulture;
    private bool connCaseSensitive;
    private Regex expression;

    internal PatternFinder(int lineNo, int symbolNo, string pattern, string escapeCharacter, LocalSQLConnection conn)
    {
      this.lineNo = lineNo;
      this.symbolNo = symbolNo;
      this.pattern = pattern;
      this.escapeCharacter = escapeCharacter;
      chunks = new List<Chunk>();
      alwaysOne = true;
      connCulture = conn.Database.Culture;
      connCaseSensitive = conn.Database.CaseSensitive;
      expression = null;
      ParsePattern();
      for (int index = 0; index < chunks.Count; ++index)
      {
        if (chunks[index].Type != ChunkType.AnyCharacters)
        {
          alwaysOne = false;
          break;
        }
      }
    }

    internal PatternFinder(string pattern, LocalSQLConnection conn)
    {
      this.pattern = pattern;
      chunks = new List<Chunk>();
      connCulture = conn.Database.Culture;
      connCaseSensitive = conn.Database.CaseSensitive;
      chunks.Add(new Chunk(ChunkType.String, pattern, connCaseSensitive, connCulture));
    }

    private void ParseRegExChunk(Chunk chunk, int index, ref StringBuilder exp, ref bool inAny, ref int minAny, ref bool openStart, ref bool openEnd)
    {
      switch (chunk.Type)
      {
        case ChunkType.AnyCharacters:
          if (inAny)
            break;
          inAny = true;
          if (index == 0)
          {
            openStart = true;
            break;
          }
          openEnd = true;
          break;
        case ChunkType.SingleCharacter:
          if (inAny)
          {
            if (!openEnd)
              openEnd = true;
            ++minAny;
            break;
          }
          exp.Append(".");
          break;
        case ChunkType.MultiChunk:
          for (int index1 = 0; index1 < chunk.ChunkCount; ++index1)
            ParseRegExChunk(chunk[index1], index + index1, ref exp, ref inAny, ref minAny, ref openStart, ref openEnd);
          break;
        default:
          if (openEnd)
          {
            if (minAny == 0)
              exp.Append("(?:.*?)");
            else if (minAny == 1)
              exp.Append("(?:.+?)");
            else
              exp.AppendFormat("(?:.{{{0},}}?)", minAny);
            openEnd = false;
          }
          inAny = false;
          minAny = 0;
          switch (chunk.Type)
          {
            case ChunkType.String:
              exp.AppendFormat("(?:{0})", Regex.Escape(chunk.Expression));
              return;
            case ChunkType.AnyCharacters:
              return;
            case ChunkType.SingleCharacter:
              return;
            case ChunkType.IncludingCharacters:
              exp.AppendFormat("[{0}]", chunk.Expression);
              return;
            case ChunkType.ExcludingCharacters:
              exp.AppendFormat("[^{0}]", chunk.Expression);
              return;
            default:
              return;
          }
      }
    }

    private void ParseRegEx()
    {
      if (expression != null)
        return;
      bool inAny = false;
      bool openStart = false;
      bool openEnd = false;
      int minAny = 0;
      StringBuilder exp = new StringBuilder();
      for (int index = 0; index < chunks.Count; ++index)
        ParseRegExChunk(chunks[index], index, ref exp, ref inAny, ref minAny, ref openStart, ref openEnd);
      if (!openEnd)
      {
        exp.Append("$");
      }
      else
      {
        for (int index = 0; index < minAny; ++index)
          exp.Append(".");
      }
      if (!openStart)
        exp.Insert(0, "^");
      RegexOptions options = RegexOptions.None;
      if (!connCaseSensitive)
        options |= RegexOptions.IgnoreCase;
      expression = new Regex(exp.ToString(), options);
    }

    private void ParsePattern()
    {
      int startIndex1 = 0;
      string str = null;
      int index;
      for (; startIndex1 < pattern.Length; startIndex1 = index + 1)
      {
                ChunkType type = ChunkType.String;
        for (index = startIndex1; index < pattern.Length; ++index)
        {
          char ch = pattern[index];
          if (escapeCharacter == null || escapeCharacter[0] != ch && (index == 0 || escapeCharacter[0] != pattern[index - 1]))
          {
            switch (ch)
            {
              case '%':
                type = ChunkType.AnyCharacters;
                goto label_9;
              case '[':
                type = ChunkType.IncludingCharacters;
                goto label_9;
              case '_':
                type = ChunkType.SingleCharacter;
                goto label_9;
              default:
                continue;
            }
          }
        }
label_9:
        if (type != ChunkType.String && index - startIndex1 > 0)
        {
          str = GetStringWithoutEscapeSymbol(pattern.Substring(startIndex1, index - startIndex1));
                    Chunk chunk = new Chunk(ChunkType.String, str, connCaseSensitive, connCulture);
          if (chunks.Count > 0 && chunks[chunks.Count - 1].Type != ChunkType.AnyCharacters)
            chunks[chunks.Count - 1].Add(chunk);
          else
            chunks.Add(chunk);
        }
        switch (type)
        {
          case ChunkType.String:
            str = GetStringWithoutEscapeSymbol(pattern.Substring(startIndex1, index - startIndex1));
            break;
          case ChunkType.AnyCharacters:
          case ChunkType.SingleCharacter:
            str = null;
            break;
          case ChunkType.IncludingCharacters:
            int startIndex2 = index + 1;
            if (startIndex2 == pattern.Length)
              throw new VistaDBSQLException(554, "", lineNo, symbolNo);
            if (pattern[startIndex2] == '^')
            {
              type = ChunkType.ExcludingCharacters;
              ++startIndex2;
              if (startIndex2 == pattern.Length)
                throw new VistaDBSQLException(554, "", lineNo, symbolNo);
            }
            index = startIndex2;
            while (pattern[index] != ']' || index + 1 < pattern.Length && pattern[index + 1] == ']')
            {
              ++index;
              if (index == pattern.Length)
                throw new VistaDBSQLException(554, "", lineNo, symbolNo);
            }
            if (startIndex2 == index)
              throw new VistaDBSQLException(554, "", lineNo, symbolNo);
            str = pattern.Substring(startIndex2, index - startIndex2);
            if (str.Length == 3 && str[1] == '-')
            {
              str = GetCharacterSetFromRange(str);
              break;
            }
            break;
        }
                Chunk chunk1 = new Chunk(type, str, connCaseSensitive, connCulture);
        if (type != ChunkType.AnyCharacters && chunks.Count > 0 && chunks[chunks.Count - 1].Type != ChunkType.AnyCharacters)
          chunks[chunks.Count - 1].Add(chunk1);
        else
          chunks.Add(chunk1);
      }
    }

    private string GetCharacterSetFromRange(string str)
    {
      if (str[0] > str[2])
        throw new VistaDBSQLException(555, "", lineNo, symbolNo);
      StringBuilder stringBuilder = new StringBuilder();
      for (char ch = str[0]; ch <= str[2]; ++ch)
        stringBuilder.Append(ch);
      return stringBuilder.ToString();
    }

    private string GetStringWithoutEscapeSymbol(string str)
    {
      if (escapeCharacter == null)
        return str;
      StringBuilder stringBuilder = new StringBuilder();
      int startIndex = 0;
      for (int index = 0; index < str.Length; ++index)
      {
        if (str[index] == escapeCharacter[0])
        {
          if (startIndex != index)
            stringBuilder.Append(str.Substring(startIndex, index - startIndex));
          ++index;
          startIndex = index;
        }
      }
      if (startIndex <= str.Length)
        stringBuilder.Append(str.Substring(startIndex, str.Length - startIndex));
      return stringBuilder.ToString();
    }

    public int CompareWithRegEx(string matchExpr)
    {
      if (alwaysOne)
        return 1;
      ParseRegEx();
      Match match = expression.Match(matchExpr);
      if (match.Success)
        return match.Index + 1;
      return 0;
    }

    public int Compare(string matchExpr)
    {
      if (alwaysOne)
        return 1;
      int pos = 0;
      int num = 0;
      for (int index = 0; index < chunks.Count; ++index)
      {
                Chunk chunk1 = chunks[index];
        if (chunk1.Type == ChunkType.AnyCharacters)
        {
          if (num == 0 && index > 0)
            num = pos + 1;
        }
        else
        {
          int chunk2 = FindChunk(matchExpr, chunk1, ref pos, index > 0, index == chunks.Count - 1);
          if (chunk2 == 0)
            return 0;
          if (num == 0)
            num = chunk2;
        }
      }
      return num;
    }

    private static bool CharIsBreaker(string s, int position)
    {
      return FTSIndex.WordBreaker.IsWordBreaker(s, position);
    }

    internal bool ContainsPattern(string matchExpr, bool prefixSearch)
    {
            Chunk chunk = chunks[0];
      if (chunk.Expression.Length == 0)
        return matchExpr.Length == 0;
      if (matchExpr == null)
        return false;
      int num1 = matchExpr.Length - 1;
      int pos = 0;
      while (pos < num1)
      {
        int num2 = FindChunk(matchExpr, chunk, ref pos, true, false) - 1;
        if (num2 > -1 && (num2 == 0 || CharIsBreaker(matchExpr, num2 - 1)) && (prefixSearch || pos == num1 || CharIsBreaker(matchExpr, pos)))
          return true;
      }
      return false;
    }

    private int FindChunk(string matchExpr, Chunk multiChunk, ref int pos, bool canSkip, bool lastChunk)
    {
      int num1 = pos;
      int length1 = matchExpr.Length;
      int num2 = 0;
      int num3 = !canSkip ? pos + 1 : length1;
      while (pos < num3 && num2 <= 0)
      {
        num2 = 0;
        num1 = pos;
        for (int index1 = 0; index1 < multiChunk.ChunkCount; ++index1)
        {
                    Chunk chunk = multiChunk[index1];
          switch (chunk.Type)
          {
            case ChunkType.String:
              if (length1 - num1 < chunk.Expression.Length)
              {
                num2 = 0;
                index1 = multiChunk.ChunkCount;
                break;
              }
              bool flag1 = false;
              int num4 = !canSkip || index1 != 0 ? num1 : length1 - chunk.Expression.Length;
              int length2 = chunk.Expression.Length;
              int num5 = num1 + length2 - 1;
              int num6 = num4 + length2 - 1;
              while (num5 <= num6)
              {
                for (int index2 = length2 - 1; index2 > -1; --index2)
                {
                  int index3 = num5 - length2 + index2 + 1;
                  if (chunk.BmTable[matchExpr[index3], index2] != 0)
                  {
                    num5 += chunk.BmTable[matchExpr[index3], index2];
                    break;
                  }
                  if (index2 == 0)
                  {
                    flag1 = true;
                    num1 = num5 + 1;
                    if (num2 == 0)
                      num2 = num1 - length2 + 1;
                    num5 = num6 + 1;
                    break;
                  }
                }
              }
              if (!flag1)
              {
                num2 = 0;
                index1 = multiChunk.ChunkCount;
                break;
              }
              break;
            case ChunkType.SingleCharacter:
              if (num1 >= length1)
              {
                num2 = 0;
                index1 = multiChunk.ChunkCount;
                break;
              }
              if (num2 == 0)
                num2 = num1 + 1;
              ++num1;
              break;
            case ChunkType.IncludingCharacters:
              if (num1 >= length1)
              {
                num2 = 0;
                index1 = multiChunk.ChunkCount;
                break;
              }
              bool flag2 = false;
              int num7 = !canSkip || index1 != 0 ? num1 : length1 - 1;
              for (int index2 = num1; index2 <= num7; ++index2)
              {
                if (LocalSQLConnection.CharIndexOf(chunk.Expression, matchExpr[index2], connCaseSensitive, connCulture) >= 0)
                {
                  flag2 = true;
                  num1 = index2 + 1;
                  if (num2 == 0)
                  {
                    num2 = index2 + 1;
                    break;
                  }
                  break;
                }
              }
              if (!flag2)
              {
                num2 = 0;
                index1 = multiChunk.ChunkCount;
                break;
              }
              break;
            case ChunkType.ExcludingCharacters:
              if (num1 >= length1)
              {
                num2 = 0;
                index1 = multiChunk.ChunkCount;
                break;
              }
              int num8 = !canSkip || index1 != 0 ? num1 : length1 - 1;
              for (int index2 = num1; index2 <= num8; ++index2)
              {
                if (LocalSQLConnection.CharIndexOf(chunk.Expression, matchExpr[index2], connCaseSensitive, connCulture) >= 0)
                {
                  num2 = 0;
                  index1 = multiChunk.ChunkCount;
                }
              }
              if (num2 == 0)
                num2 = num1 + 1;
              num1 = num8 + 1;
              break;
          }
        }
        if (lastChunk && num1 < length1)
          num2 = 0;
        ++pos;
      }
      pos = num1;
      return num2;
    }

    public OptimizationLevel GetOptimizationLevel(out int chunkCount)
    {
      chunkCount = chunks.Count;
      return chunks.Count == 0 || chunks[0].Type != ChunkType.String || chunks.Count != 1 && (chunks.Count != 2 || chunks[1].Type != ChunkType.AnyCharacters) ? OptimizationLevel.None : OptimizationLevel.Full;
    }

    internal void GetOptimizationScopeSignatures(Statement statement, out Signature low, out Signature high)
    {
            Chunk chunk = chunks[0];
      IDatabase database = statement.Database;
      IColumn emtpyUnicodeColumn1 = database.CreateEmtpyUnicodeColumn();
      IColumn emtpyUnicodeColumn2 = database.CreateEmtpyUnicodeColumn();
      string expression = chunk[0].Expression;
            emtpyUnicodeColumn1.Value = expression;
            emtpyUnicodeColumn2.Value = expression + database.MaximumChar;
      low = ConstantSignature.CreateSignature(emtpyUnicodeColumn1, statement);
      high = ConstantSignature.CreateSignature(emtpyUnicodeColumn2, statement);
    }

    private enum ChunkType
    {
      String,
      AnyCharacters,
      SingleCharacter,
      IncludingCharacters,
      ExcludingCharacters,
      MultiChunk,
    }

    private class Chunk
    {
      private ChunkType type;
      private string expression;
      private List<Chunk> chunks;
      private OffsetTable bmTable;
      private bool caseSensitive;
      private CultureInfo cultureInfo;

      public Chunk(ChunkType type, string expression, bool caseSensitive, CultureInfo cultureInfo)
      {
        this.type = type;
        this.expression = expression;
        this.caseSensitive = caseSensitive;
        this.cultureInfo = cultureInfo;
        if (this.type != ChunkType.String)
          return;
        bmTable = new OffsetTable(expression, caseSensitive, cultureInfo);
      }

      public void Add(Chunk chunk)
      {
        if (type != ChunkType.MultiChunk)
        {
          chunks = new List<Chunk>();
          chunks.Add(new Chunk(type, expression, caseSensitive, cultureInfo));
          type = ChunkType.MultiChunk;
        }
        chunks.Add(chunk);
      }

      public ChunkType Type
      {
        get
        {
          return type;
        }
      }

      public string Expression
      {
        get
        {
          return expression;
        }
      }

      public Chunk this[int i]
      {
        get
        {
          if (chunks != null)
            return chunks[i];
          return this;
        }
      }

      public int ChunkCount
      {
        get
        {
          if (chunks != null)
            return chunks.Count;
          return 1;
        }
      }

      internal OffsetTable BmTable
      {
        get
        {
          return bmTable;
        }
      }
    }

    internal class OffsetTable
    {
      private int[,] offsetTable;

      private void InitOffsetTable(int patternLength)
      {
        offsetTable = new int[ushort.MaxValue, patternLength];
      }

      private static int FindRightMost(string s, string p, int n)
      {
        int length = p.Length;
        if (length > n)
          return -1;
        for (int index1 = n - length; index1 > -1; --index1)
        {
          for (int index2 = 0; index2 < length && s[index1 + index2] == p[index2]; ++index2)
          {
            if (index2 == length - 1)
              return index1;
          }
        }
        return -1;
      }

      internal OffsetTable(string pattern, bool caseSensitive, CultureInfo culture)
      {
        if (!caseSensitive)
          pattern = pattern.ToUpper(culture);
        int length = pattern.Length;
        InitOffsetTable(length);
        if (pattern == null || pattern.Length == 0)
          return;
        for (int index = 0; index < ushort.MaxValue; ++index)
          offsetTable[index, length - 1] = length;
        for (int index = length - 1; index > -1; --index)
        {
          if (offsetTable[pattern[index], length - 1] == length)
          {
            offsetTable[pattern[index], length - 1] = length - index - 1;
            if (!caseSensitive && char.IsLetter(pattern[index]))
              offsetTable[char.ToLower(pattern[index], culture), length - 1] = length - index - 1;
          }
        }
        int num1 = length;
        for (int index1 = length - 2; index1 > -1; --index1)
        {
          string str = pattern.Substring(index1 + 1);
          if (pattern.IndexOf(str) == 0)
            num1 = index1 + 1;
          for (int index2 = 0; index2 < ushort.MaxValue; ++index2)
          {
            char c = (char) index2;
            if (caseSensitive || !char.IsLetter(c) || !char.IsLower(c))
            {
              int num2 = num1;
              string p = ((int) c).ToString() + pattern.Substring(index1 + 1);
              int rightMost = FindRightMost(pattern, p, length - 1);
              if (rightMost > -1)
                num2 = index1 - rightMost;
              offsetTable[index2, index1] = num2;
              if (!caseSensitive && char.IsLetter(c))
                offsetTable[char.ToLower(c, culture), index1] = num2;
            }
          }
          offsetTable[pattern[index1], index1] = 0;
          if (!caseSensitive && char.IsLetter(pattern[index1]))
            offsetTable[char.ToLower(pattern[index1], culture), index1] = 0;
        }
      }

      internal int this[int rowNo, int colNo]
      {
        get
        {
          return offsetTable[rowNo, colNo];
        }
      }
    }
  }
}
