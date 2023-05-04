using System.Globalization;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core
{
  internal class TranslationList : InsensitiveHashtable
  {
    internal TranslationList()
    {
    }

    internal Rule this[Row.Column srcColumn]
    {
      get
      {
        if (!Contains((object) srcColumn.Name))
          return (Rule) null;
        return (Rule) this[(object) srcColumn.Name];
      }
    }

    internal void AddTranslationRule(Row.Column srcColumn, Row.Column dstColumn)
    {
      Add((object) srcColumn.Name, (object) new Rule(srcColumn, dstColumn, CrossConversion.Method(srcColumn.InternalType, dstColumn.InternalType)));
    }

    internal void AddTranslationRule(DefaultValue dstDefaults, Row.Column dstColumn)
    {
      Add((object) ("New_" + dstColumn.Name), (object) new Rule(dstDefaults, dstColumn, (CrossConversion.ConversionMethod) null));
    }

    internal void DropTranslationRule(Row.Column srcColumn)
    {
      string name = srcColumn.Name;
      if (!Contains((object) name))
        return;
      Remove((object) name);
    }

    internal bool IsColumnTranslated(Row.Column srcColumn)
    {
      return Contains((object) srcColumn.Name);
    }

    protected virtual void Destroy()
    {
    }

    internal class Rule
    {
      private Row.Column srcColumn;
      private Row.Column dstColumn;
      private DefaultValue srcValue;
      private CrossConversion.ConversionMethod method;

      internal Rule(Row.Column srcColumn, Row.Column dstColumn, CrossConversion.ConversionMethod method)
      {
        this.srcColumn = srcColumn;
        this.dstColumn = dstColumn;
        this.method = method;
      }

      internal Rule(DefaultValue srcValue, Row.Column dstColumn, CrossConversion.ConversionMethod method)
      {
        this.srcValue = srcValue;
        this.dstColumn = dstColumn;
        this.method = method;
      }

      internal Row.Column Source
      {
        get
        {
          return srcColumn;
        }
      }

      internal Row.Column Destination
      {
        get
        {
          return dstColumn;
        }
      }

      internal void Convert(Row srcRow, Row dstRow, CultureInfo culture)
      {
        if (srcValue != null && srcColumn == (Row.Column) null)
          srcValue.GetValidRowStatus(dstRow);
        else if (srcColumn.InternalType == dstColumn.InternalType)
        {
          dstRow[dstColumn.RowIndex].Value = srcRow[srcColumn.RowIndex].Value;
        }
        else
        {
          if (method == null)
            return;
          method((IValue) srcRow[srcColumn.RowIndex], (IValue) dstRow[dstColumn.RowIndex], culture);
        }
      }
    }
  }
}
