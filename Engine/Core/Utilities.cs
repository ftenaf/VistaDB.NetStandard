using System;
using System.Text;

namespace VistaDB.Engine.Core
{
  internal static class Utilities
  {
    internal static string ReplaceStringEx(string original, string pattern, string replacement, StringComparison comparisonType, int stringBuilderInitialSize)
    {
      if (original == null)
        return (string) null;
      if (string.IsNullOrEmpty(pattern))
        return original;
      int startIndex = 0;
      int length = pattern.Length;
      int num = original.IndexOf(pattern, comparisonType);
      StringBuilder stringBuilder = new StringBuilder(stringBuilderInitialSize < 0 ? Math.Min(4096, original.Length) : stringBuilderInitialSize);
      for (; num >= 0; num = original.IndexOf(pattern, startIndex, comparisonType))
      {
        stringBuilder.Append(original, startIndex, num - startIndex);
        stringBuilder.Append(replacement);
        startIndex = num + length;
      }
      stringBuilder.Append(original, startIndex, original.Length - startIndex);
      return stringBuilder.ToString();
    }
  }
}
