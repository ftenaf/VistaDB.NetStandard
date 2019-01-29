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
    private List<PatternFinder.Chunk> chunks;
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
      this.chunks = new List<PatternFinder.Chunk>();
      this.alwaysOne = true;
      this.connCulture = conn.Database.Culture;
      this.connCaseSensitive = conn.Database.CaseSensitive;
      this.expression = (Regex) null;
      this.ParsePattern();
      for (int index = 0; index < this.chunks.Count; ++index)
      {
        if (this.chunks[index].Type != PatternFinder.ChunkType.AnyCharacters)
        {
          this.alwaysOne = false;
          break;
        }
      }
    }

    internal PatternFinder(string pattern, LocalSQLConnection conn)
    {
      this.pattern = pattern;
      this.chunks = new List<PatternFinder.Chunk>();
      this.connCulture = conn.Database.Culture;
      this.connCaseSensitive = conn.Database.CaseSensitive;
      this.chunks.Add(new PatternFinder.Chunk(PatternFinder.ChunkType.String, pattern, this.connCaseSensitive, this.connCulture));
    }

    private void ParseRegExChunk(PatternFinder.Chunk chunk, int index, ref StringBuilder exp, ref bool inAny, ref int minAny, ref bool openStart, ref bool openEnd)
    {
      switch (chunk.Type)
      {
        case PatternFinder.ChunkType.AnyCharacters:
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
        case PatternFinder.ChunkType.SingleCharacter:
          if (inAny)
          {
            if (!openEnd)
              openEnd = true;
            ++minAny;
            break;
          }
          exp.Append(".");
          break;
        case PatternFinder.ChunkType.MultiChunk:
          for (int index1 = 0; index1 < chunk.ChunkCount; ++index1)
            this.ParseRegExChunk(chunk[index1], index + index1, ref exp, ref inAny, ref minAny, ref openStart, ref openEnd);
          break;
        default:
          if (openEnd)
          {
            if (minAny == 0)
              exp.Append("(?:.*?)");
            else if (minAny == 1)
              exp.Append("(?:.+?)");
            else
              exp.AppendFormat("(?:.{{{0},}}?)", (object) minAny);
            openEnd = false;
          }
          inAny = false;
          minAny = 0;
          switch (chunk.Type)
          {
            case PatternFinder.ChunkType.String:
              exp.AppendFormat("(?:{0})", (object) Regex.Escape(chunk.Expression));
              return;
            case PatternFinder.ChunkType.AnyCharacters:
              return;
            case PatternFinder.ChunkType.SingleCharacter:
              return;
            case PatternFinder.ChunkType.IncludingCharacters:
              exp.AppendFormat("[{0}]", (object) chunk.Expression);
              return;
            case PatternFinder.ChunkType.ExcludingCharacters:
              exp.AppendFormat("[^{0}]", (object) chunk.Expression);
              return;
            default:
              return;
          }
      }
    }

    private void ParseRegEx()
    {
      if (this.expression != null)
        return;
      bool inAny = false;
      bool openStart = false;
      bool openEnd = false;
      int minAny = 0;
      StringBuilder exp = new StringBuilder();
      for (int index = 0; index < this.chunks.Count; ++index)
        this.ParseRegExChunk(this.chunks[index], index, ref exp, ref inAny, ref minAny, ref openStart, ref openEnd);
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
      if (!this.connCaseSensitive)
        options |= RegexOptions.IgnoreCase;
      this.expression = new Regex(exp.ToString(), options);
    }

    private void ParsePattern()
    {
      int startIndex1 = 0;
      string str = (string) null;
      int index;
      for (; startIndex1 < this.pattern.Length; startIndex1 = index + 1)
      {
        PatternFinder.ChunkType type = PatternFinder.ChunkType.String;
        for (index = startIndex1; index < this.pattern.Length; ++index)
        {
          char ch = this.pattern[index];
          if (this.escapeCharacter == null || (int) this.escapeCharacter[0] != (int) ch && (index == 0 || (int) this.escapeCharacter[0] != (int) this.pattern[index - 1]))
          {
            switch (ch)
            {
              case '%':
                type = PatternFinder.ChunkType.AnyCharacters;
                goto label_9;
              case '[':
                type = PatternFinder.ChunkType.IncludingCharacters;
                goto label_9;
              case '_':
                type = PatternFinder.ChunkType.SingleCharacter;
                goto label_9;
              default:
                continue;
            }
          }
        }
label_9:
        if (type != PatternFinder.ChunkType.String && index - startIndex1 > 0)
        {
          str = this.GetStringWithoutEscapeSymbol(this.pattern.Substring(startIndex1, index - startIndex1));
          PatternFinder.Chunk chunk = new PatternFinder.Chunk(PatternFinder.ChunkType.String, str, this.connCaseSensitive, this.connCulture);
          if (this.chunks.Count > 0 && this.chunks[this.chunks.Count - 1].Type != PatternFinder.ChunkType.AnyCharacters)
            this.chunks[this.chunks.Count - 1].Add(chunk);
          else
            this.chunks.Add(chunk);
        }
        switch (type)
        {
          case PatternFinder.ChunkType.String:
            str = this.GetStringWithoutEscapeSymbol(this.pattern.Substring(startIndex1, index - startIndex1));
            break;
          case PatternFinder.ChunkType.AnyCharacters:
          case PatternFinder.ChunkType.SingleCharacter:
            str = (string) null;
            break;
          case PatternFinder.ChunkType.IncludingCharacters:
            int startIndex2 = index + 1;
            if (startIndex2 == this.pattern.Length)
              throw new VistaDBSQLException(554, "", this.lineNo, this.symbolNo);
            if (this.pattern[startIndex2] == '^')
            {
              type = PatternFinder.ChunkType.ExcludingCharacters;
              ++startIndex2;
              if (startIndex2 == this.pattern.Length)
                throw new VistaDBSQLException(554, "", this.lineNo, this.symbolNo);
            }
            index = startIndex2;
            while (this.pattern[index] != ']' || index + 1 < this.pattern.Length && this.pattern[index + 1] == ']')
            {
              ++index;
              if (index == this.pattern.Length)
                throw new VistaDBSQLException(554, "", this.lineNo, this.symbolNo);
            }
            if (startIndex2 == index)
              throw new VistaDBSQLException(554, "", this.lineNo, this.symbolNo);
            str = this.pattern.Substring(startIndex2, index - startIndex2);
            if (str.Length == 3 && str[1] == '-')
            {
              str = this.GetCharacterSetFromRange(str);
              break;
            }
            break;
        }
        PatternFinder.Chunk chunk1 = new PatternFinder.Chunk(type, str, this.connCaseSensitive, this.connCulture);
        if (type != PatternFinder.ChunkType.AnyCharacters && this.chunks.Count > 0 && this.chunks[this.chunks.Count - 1].Type != PatternFinder.ChunkType.AnyCharacters)
          this.chunks[this.chunks.Count - 1].Add(chunk1);
        else
          this.chunks.Add(chunk1);
      }
    }

    private string GetCharacterSetFromRange(string str)
    {
      if ((int) str[0] > (int) str[2])
        throw new VistaDBSQLException(555, "", this.lineNo, this.symbolNo);
      StringBuilder stringBuilder = new StringBuilder();
      for (char ch = str[0]; (int) ch <= (int) str[2]; ++ch)
        stringBuilder.Append(ch);
      return stringBuilder.ToString();
    }

    private string GetStringWithoutEscapeSymbol(string str)
    {
      if (this.escapeCharacter == null)
        return str;
      StringBuilder stringBuilder = new StringBuilder();
      int startIndex = 0;
      for (int index = 0; index < str.Length; ++index)
      {
        if ((int) str[index] == (int) this.escapeCharacter[0])
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
      if (this.alwaysOne)
        return 1;
      this.ParseRegEx();
      Match match = this.expression.Match(matchExpr);
      if (match.Success)
        return match.Index + 1;
      return 0;
    }

    public int Compare(string matchExpr)
    {
      if (this.alwaysOne)
        return 1;
      int pos = 0;
      int num = 0;
      for (int index = 0; index < this.chunks.Count; ++index)
      {
        PatternFinder.Chunk chunk1 = this.chunks[index];
        if (chunk1.Type == PatternFinder.ChunkType.AnyCharacters)
        {
          if (num == 0 && index > 0)
            num = pos + 1;
        }
        else
        {
          int chunk2 = this.FindChunk(matchExpr, chunk1, ref pos, index > 0, index == this.chunks.Count - 1);
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
      PatternFinder.Chunk chunk = this.chunks[0];
      if (chunk.Expression.Length == 0)
        return matchExpr.Length == 0;
      if (matchExpr == null)
        return false;
      int num1 = matchExpr.Length - 1;
      int pos = 0;
      while (pos < num1)
      {
        int num2 = this.FindChunk(matchExpr, chunk, ref pos, true, false) - 1;
        if (num2 > -1 && (num2 == 0 || PatternFinder.CharIsBreaker(matchExpr, num2 - 1)) && (prefixSearch || pos == num1 || PatternFinder.CharIsBreaker(matchExpr, pos)))
          return true;
      }
      return false;
    }

    private int FindChunk(string matchExpr, PatternFinder.Chunk multiChunk, ref int pos, bool canSkip, bool lastChunk)
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
          PatternFinder.Chunk chunk = multiChunk[index1];
          switch (chunk.Type)
          {
            case PatternFinder.ChunkType.String:
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
                  if (chunk.BmTable[(int) matchExpr[index3], index2] != 0)
                  {
                    num5 += chunk.BmTable[(int) matchExpr[index3], index2];
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
            case PatternFinder.ChunkType.SingleCharacter:
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
            case PatternFinder.ChunkType.IncludingCharacters:
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
                if (LocalSQLConnection.CharIndexOf(chunk.Expression, matchExpr[index2], this.connCaseSensitive, this.connCulture) >= 0)
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
            case PatternFinder.ChunkType.ExcludingCharacters:
              if (num1 >= length1)
              {
                num2 = 0;
                index1 = multiChunk.ChunkCount;
                break;
              }
              int num8 = !canSkip || index1 != 0 ? num1 : length1 - 1;
              for (int index2 = num1; index2 <= num8; ++index2)
              {
                if (LocalSQLConnection.CharIndexOf(chunk.Expression, matchExpr[index2], this.connCaseSensitive, this.connCulture) >= 0)
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
      chunkCount = this.chunks.Count;
      return this.chunks.Count == 0 || this.chunks[0].Type != PatternFinder.ChunkType.String || this.chunks.Count != 1 && (this.chunks.Count != 2 || this.chunks[1].Type != PatternFinder.ChunkType.AnyCharacters) ? OptimizationLevel.None : OptimizationLevel.Full;
    }

    internal void GetOptimizationScopeSignatures(Statement statement, out Signature low, out Signature high)
    {
      PatternFinder.Chunk chunk = this.chunks[0];
      IDatabase database = statement.Database;
      IColumn emtpyUnicodeColumn1 = database.CreateEmtpyUnicodeColumn();
      IColumn emtpyUnicodeColumn2 = database.CreateEmtpyUnicodeColumn();
      string expression = chunk[0].Expression;
      ((IValue) emtpyUnicodeColumn1).Value = (object) expression;
      ((IValue) emtpyUnicodeColumn2).Value = (object) (expression + (object) database.MaximumChar);
      low = (Signature) ConstantSignature.CreateSignature(emtpyUnicodeColumn1, statement);
      high = (Signature) ConstantSignature.CreateSignature(emtpyUnicodeColumn2, statement);
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
      private PatternFinder.ChunkType type;
      private string expression;
      private List<PatternFinder.Chunk> chunks;
      private PatternFinder.OffsetTable bmTable;
      private bool caseSensitive;
      private CultureInfo cultureInfo;

      public Chunk(PatternFinder.ChunkType type, string expression, bool caseSensitive, CultureInfo cultureInfo)
      {
        this.type = type;
        this.expression = expression;
        this.caseSensitive = caseSensitive;
        this.cultureInfo = cultureInfo;
        if (this.type != PatternFinder.ChunkType.String)
          return;
        this.bmTable = new PatternFinder.OffsetTable(expression, caseSensitive, cultureInfo);
      }

      public void Add(PatternFinder.Chunk chunk)
      {
        if (this.type != PatternFinder.ChunkType.MultiChunk)
        {
          this.chunks = new List<PatternFinder.Chunk>();
          this.chunks.Add(new PatternFinder.Chunk(this.type, this.expression, this.caseSensitive, this.cultureInfo));
          this.type = PatternFinder.ChunkType.MultiChunk;
        }
        this.chunks.Add(chunk);
      }

      public PatternFinder.ChunkType Type
      {
        get
        {
          return this.type;
        }
      }

      public string Expression
      {
        get
        {
          return this.expression;
        }
      }

      public PatternFinder.Chunk this[int i]
      {
        get
        {
          if (this.chunks != null)
            return this.chunks[i];
          return this;
        }
      }

      public int ChunkCount
      {
        get
        {
          if (this.chunks != null)
            return this.chunks.Count;
          return 1;
        }
      }

      internal PatternFinder.OffsetTable BmTable
      {
        get
        {
          return this.bmTable;
        }
      }
    }

    internal class OffsetTable
    {
      private int[,] offsetTable;

      private void InitOffsetTable(int patternLength)
      {
        this.offsetTable = new int[(int) ushort.MaxValue, patternLength];
      }

      private static int FindRightMost(string s, string p, int n)
      {
        int length = p.Length;
        if (length > n)
          return -1;
        for (int index1 = n - length; index1 > -1; --index1)
        {
          for (int index2 = 0; index2 < length && (int) s[index1 + index2] == (int) p[index2]; ++index2)
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
        this.InitOffsetTable(length);
        if (pattern == null || pattern.Length == 0)
          return;
        for (int index = 0; index < (int) ushort.MaxValue; ++index)
          this.offsetTable[index, length - 1] = length;
        for (int index = length - 1; index > -1; --index)
        {
          if (this.offsetTable[(int) pattern[index], length - 1] == length)
          {
            this.offsetTable[(int) pattern[index], length - 1] = length - index - 1;
            if (!caseSensitive && char.IsLetter(pattern[index]))
              this.offsetTable[(int) char.ToLower(pattern[index], culture), length - 1] = length - index - 1;
          }
        }
        int num1 = length;
        for (int index1 = length - 2; index1 > -1; --index1)
        {
          string str = pattern.Substring(index1 + 1);
          if (pattern.IndexOf(str) == 0)
            num1 = index1 + 1;
          for (int index2 = 0; index2 < (int) ushort.MaxValue; ++index2)
          {
            char c = (char) index2;
            if (caseSensitive || !char.IsLetter(c) || !char.IsLower(c))
            {
              int num2 = num1;
              string p = ((int) c).ToString() + pattern.Substring(index1 + 1);
              int rightMost = PatternFinder.OffsetTable.FindRightMost(pattern, p, length - 1);
              if (rightMost > -1)
                num2 = index1 - rightMost;
              this.offsetTable[index2, index1] = num2;
              if (!caseSensitive && char.IsLetter(c))
                this.offsetTable[(int) char.ToLower(c, culture), index1] = num2;
            }
          }
          this.offsetTable[(int) pattern[index1], index1] = 0;
          if (!caseSensitive && char.IsLetter(pattern[index1]))
            this.offsetTable[(int) char.ToLower(pattern[index1], culture), index1] = 0;
        }
      }

      internal int this[int rowNo, int colNo]
      {
        get
        {
          return this.offsetTable[rowNo, colNo];
        }
      }
    }
  }
}
