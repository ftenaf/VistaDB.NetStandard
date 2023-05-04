using System;
using System.ComponentModel;
using VistaDB.DDA;
using VistaDB.Diagnostic;

namespace VistaDB.Extra.Internal
{
  internal class DataTablePropertyDescriptor : PropertyDescriptor
  {
    private Type propertyType;
    private int columnNo;
    private VistaDBType dataType;
    private bool readOnly;

    internal DataTablePropertyDescriptor(string name, Type propertyType, int columnNo, VistaDBType dataType, bool readOnly)
      : base(name, null)
    {
      this.propertyType = propertyType;
      this.columnNo = columnNo;
      this.dataType = dataType;
      this.readOnly = readOnly;
    }

    public override Type ComponentType
    {
      get
      {
        return typeof (EditableRow);
      }
    }

    public override object GetValue(object component)
    {
      try
      {
        IVistaDBColumn vistaDbColumn = ((EditableRow) component).Row[columnNo];
        return vistaDbColumn.Value == null ? DBNull.Value : vistaDbColumn.Value;
      }
      catch (Exception ex)
      {
        throw new VistaDBDataTableException(ex, 2041);
      }
    }

    public override bool IsReadOnly
    {
      get
      {
        return readOnly;
      }
    }

    public override Type PropertyType
    {
      get
      {
        return propertyType;
      }
    }

    public override bool CanResetValue(object component)
    {
      return true;
    }

    public override void ResetValue(object component)
    {
    }

    public override void SetValue(object component, object value)
    {
      ((EditableRow) component).SetDataToColumn(columnNo, value);
    }

    public override bool ShouldSerializeValue(object component)
    {
      return true;
    }
  }
}
