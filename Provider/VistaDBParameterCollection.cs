using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;

namespace VistaDB.Provider
{
  public sealed class VistaDBParameterCollection : DbParameterCollection
  {
    private List<VistaDBParameter> parameters;

    internal VistaDBParameterCollection()
    {
      this.parameters = new List<VistaDBParameter>();
    }

    public override int Count
    {
      get
      {
        return this.parameters.Count;
      }
    }

    public override bool IsFixedSize
    {
      get
      {
        return false;
      }
    }

    public override bool IsReadOnly
    {
      get
      {
        return false;
      }
    }

    public override bool IsSynchronized
    {
      get
      {
        return true;
      }
    }

    public VistaDBParameter this[int index]
    {
      get
      {
        return this.parameters[index];
      }
      set
      {
        this.parameters[index] = value;
      }
    }

    public VistaDBParameter this[string parameterName]
    {
      get
      {
        int parameter = this.FindParameter(parameterName);
        if (parameter >= 0)
          return this.parameters[parameter];
        return (VistaDBParameter) null;
      }
      set
      {
        this.parameters[this.FindParameter(parameterName)] = value;
      }
    }

    public override object SyncRoot
    {
      get
      {
        return (object) this;
      }
    }

    public override int Add(object value)
    {
      this.parameters.Add((VistaDBParameter) value);
      return this.parameters.Count - 1;
    }

    public VistaDBParameter Add(VistaDBParameter parameter)
    {
      this.Add((object) parameter);
      return parameter;
    }

    public VistaDBParameter Add(string parameterName, object value)
    {
      return this.Add(new VistaDBParameter(parameterName, value));
    }

    public VistaDBParameter Add(string parameterName, VistaDBType dataType)
    {
      return this.Add(new VistaDBParameter(parameterName, dataType));
    }

    public VistaDBParameter Add(string parameterName, VistaDBType dataType, int size)
    {
      return this.Add(new VistaDBParameter(parameterName, dataType, size));
    }

    public VistaDBParameter Add(string parameterName, VistaDBType dataType, int size, string sourceColumn)
    {
      return this.Add(new VistaDBParameter(parameterName, dataType, size, sourceColumn));
    }

    public VistaDBParameter AddWithValue(string parameterName, object value)
    {
      return this.Add(new VistaDBParameter(parameterName, value));
    }

    public override void AddRange(Array values)
    {
      int index = 0;
      for (int length = values.Length; index < length; ++index)
        this.parameters.Add((VistaDBParameter) values.GetValue(index));
    }

    public override void Clear()
    {
      this.parameters.Clear();
    }

    public override bool Contains(object value)
    {
      return this.parameters.Contains((VistaDBParameter) value);
    }

    public override bool Contains(string parameterName)
    {
      return this.FindParameter(parameterName) >= 0;
    }

    public override void CopyTo(Array array, int index)
    {
      ((ICollection) this.parameters).CopyTo(array, index);
    }

    public override IEnumerator GetEnumerator()
    {
      return (IEnumerator) new VistaDBParameterCollection.ParameterEnumerator(this);
    }

    public override int IndexOf(object value)
    {
      return this.parameters.IndexOf((VistaDBParameter) value);
    }

    public override int IndexOf(string parameterName)
    {
      return this.FindParameter(parameterName);
    }

    public override void Insert(int index, object value)
    {
      this.parameters.Insert(index, (VistaDBParameter) value);
    }

    public void Insert(int index, VistaDBParameter parameter)
    {
      this.parameters.Insert(index, parameter);
    }

    public override void Remove(object value)
    {
      this.parameters.Remove((VistaDBParameter) value);
    }

    public void Remove(VistaDBParameter parameter)
    {
      this.parameters.Remove(parameter);
    }

    public override void RemoveAt(int index)
    {
      this.parameters.RemoveAt(index);
    }

    public override void RemoveAt(string parameterName)
    {
      this.parameters.RemoveAt(this.FindParameter(parameterName));
    }

    protected override DbParameter GetParameter(int index)
    {
      return (DbParameter) this[index];
    }

    protected override DbParameter GetParameter(string parameterName)
    {
      return (DbParameter) this[parameterName];
    }

    protected override void SetParameter(int index, DbParameter value)
    {
      this[index] = (VistaDBParameter) value;
    }

    protected override void SetParameter(string parameterName, DbParameter value)
    {
      this[parameterName] = (VistaDBParameter) value;
    }

    private int FindParameter(string name)
    {
      name = name.ToUpperInvariant();
      for (int index = 0; index < this.parameters.Count; ++index)
      {
        if (string.Compare(this.parameters[index].ParameterName, name, StringComparison.OrdinalIgnoreCase) == 0)
          return index;
      }
      return -1;
    }

    public class ParameterEnumerator : IEnumerator
    {
      private VistaDBParameterCollection parent;
      private int index;

      internal ParameterEnumerator(VistaDBParameterCollection parent)
      {
        this.parent = parent;
        this.index = -1;
      }

      public object Current
      {
        get
        {
          if (this.index >= 0)
            return (object) this.parent.parameters[this.index];
          return (object) null;
        }
      }

      public bool MoveNext()
      {
        if (this.index == this.parent.parameters.Count - 1)
          return false;
        ++this.index;
        return true;
      }

      public void Reset()
      {
        this.index = -1;
      }
    }
  }
}
