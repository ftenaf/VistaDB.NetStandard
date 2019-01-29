using System;
using System.Reflection;

[AttributeUsage(AttributeTargets.Assembly, AllowMultiple = true)]
[Obfuscation(ApplyToMembers = true, Exclude = true)]
internal sealed class ConnectionStringAttribute : Attribute
{
  private string _name;
  private string _value;
  private string[] _names;

  public ConnectionStringAttribute(string name, string value)
  {
    this._name = name;
    this._value = value;
    this._names = new string[0];
  }

  public ConnectionStringAttribute(string name, string value, params string[] alternateNames)
  {
    this._name = name;
    this._value = value;
    this._names = alternateNames;
  }

  public string Name
  {
    get
    {
      return this._name;
    }
  }

  public string Value
  {
    get
    {
      return this._value;
    }
  }

  public string[] AlternateNames
  {
    get
    {
      return this._names;
    }
  }
}
