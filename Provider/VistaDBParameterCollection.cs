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
      parameters = new List<VistaDBParameter>();
    }

    public override int Count
    {
      get
      {
        return parameters.Count;
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

    new public VistaDBParameter this[int index]
    {
      get
      {
        return parameters[index];
      }
      set
      {
        parameters[index] = value;
      }
    }

    new public VistaDBParameter this[string parameterName]
    {
      get
      {
        int parameter = FindParameter(parameterName);
        if (parameter >= 0)
          return parameters[parameter];
        return (VistaDBParameter) null;
      }
      set
      {
        parameters[FindParameter(parameterName)] = value;
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
      parameters.Add((VistaDBParameter) value);
      return parameters.Count - 1;
    }

    public VistaDBParameter Add(VistaDBParameter parameter)
    {
      Add((object) parameter);
      return parameter;
    }

    public VistaDBParameter Add(string parameterName, object value)
    {
      return Add(new VistaDBParameter(parameterName, value));
    }

    public VistaDBParameter Add(string parameterName, VistaDBType dataType)
    {
      return Add(new VistaDBParameter(parameterName, dataType));
    }

    public VistaDBParameter Add(string parameterName, VistaDBType dataType, int size)
    {
      return Add(new VistaDBParameter(parameterName, dataType, size));
    }

    public VistaDBParameter Add(string parameterName, VistaDBType dataType, int size, string sourceColumn)
    {
      return Add(new VistaDBParameter(parameterName, dataType, size, sourceColumn));
    }

    public VistaDBParameter AddWithValue(string parameterName, object value)
    {
      return Add(new VistaDBParameter(parameterName, value));
    }

    public override void AddRange(Array values)
    {
      int index = 0;
      for (int length = values.Length; index < length; ++index)
        parameters.Add((VistaDBParameter) values.GetValue(index));
    }

    public override void Clear()
    {
      parameters.Clear();
    }

    public override bool Contains(object value)
    {
      return parameters.Contains((VistaDBParameter) value);
    }

    public override bool Contains(string parameterName)
    {
      return FindParameter(parameterName) >= 0;
    }

    public override void CopyTo(Array array, int index)
    {
      ((ICollection) parameters).CopyTo(array, index);
    }

    public override IEnumerator GetEnumerator()
    {
      return (IEnumerator) new ParameterEnumerator(this);
    }

    public override int IndexOf(object value)
    {
      return parameters.IndexOf((VistaDBParameter) value);
    }

    public override int IndexOf(string parameterName)
    {
      return FindParameter(parameterName);
    }

    public override void Insert(int index, object value)
    {
      parameters.Insert(index, (VistaDBParameter) value);
    }

    public void Insert(int index, VistaDBParameter parameter)
    {
      parameters.Insert(index, parameter);
    }

    public override void Remove(object value)
    {
      parameters.Remove((VistaDBParameter) value);
    }

    public void Remove(VistaDBParameter parameter)
    {
      parameters.Remove(parameter);
    }

    public override void RemoveAt(int index)
    {
      parameters.RemoveAt(index);
    }

    public override void RemoveAt(string parameterName)
    {
      parameters.RemoveAt(FindParameter(parameterName));
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
      for (int index = 0; index < parameters.Count; ++index)
      {
        if (string.Compare(parameters[index].ParameterName, name, StringComparison.OrdinalIgnoreCase) == 0)
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
        index = -1;
      }

      public object Current
      {
        get
        {
          if (index >= 0)
            return (object) parent.parameters[index];
          return (object) null;
        }
      }

      public bool MoveNext()
      {
        if (index == parent.parameters.Count - 1)
          return false;
        ++index;
        return true;
      }

      public void Reset()
      {
        index = -1;
      }
    }
  }
}
