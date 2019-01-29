using System.Globalization;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core
{
  internal class TranslationList : InsensitiveHashtable
  {
    internal TranslationList()
    {
    }

    internal TranslationList.Rule this[Row.Column srcColumn]
    {
      get
      {
        if (!this.Contains((object) srcColumn.Name))
          return (TranslationList.Rule) null;
        return (TranslationList.Rule) this[(object) srcColumn.Name];
      }
    }

    internal void AddTranslationRule(Row.Column srcColumn, Row.Column dstColumn)
    {
      this.Add((object) srcColumn.Name, (object) new TranslationList.Rule(srcColumn, dstColumn, CrossConversion.Method(srcColumn.InternalType, dstColumn.InternalType)));
    }

    internal void AddTranslationRule(DefaultValue dstDefaults, Row.Column dstColumn)
    {
      this.Add((object) ("New_" + dstColumn.Name), (object) new TranslationList.Rule(dstDefaults, dstColumn, (CrossConversion.ConversionMethod) null));
    }

    internal void DropTranslationRule(Row.Column srcColumn)
    {
      string name = srcColumn.Name;
      if (!this.Contains((object) name))
        return;
      this.Remove((object) name);
    }

    internal bool IsColumnTranslated(Row.Column srcColumn)
    {
      return this.Contains((object) srcColumn.Name);
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
          return this.srcColumn;
        }
      }

      internal Row.Column Destination
      {
        get
        {
          return this.dstColumn;
        }
      }

      internal void Convert(Row srcRow, Row dstRow, CultureInfo culture)
      {
        if (this.srcValue != null && this.srcColumn == (Row.Column) null)
          this.srcValue.GetValidRowStatus(dstRow);
        else if (this.srcColumn.InternalType == this.dstColumn.InternalType)
        {
          dstRow[this.dstColumn.RowIndex].Value = srcRow[this.srcColumn.RowIndex].Value;
        }
        else
        {
          if (this.method == null)
            return;
          this.method((IValue) srcRow[this.srcColumn.RowIndex], (IValue) dstRow[this.dstColumn.RowIndex], culture);
        }
      }
    }
  }
}
