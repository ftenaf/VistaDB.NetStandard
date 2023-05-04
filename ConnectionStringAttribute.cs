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
    _name = name;
    _value = value;
    _names = new string[0];
  }

  public ConnectionStringAttribute(string name, string value, params string[] alternateNames)
  {
    _name = name;
    _value = value;
    _names = alternateNames;
  }

  public string Name
  {
    get
    {
      return _name;
    }
  }

  public string Value
  {
    get
    {
      return _value;
    }
  }

  public string[] AlternateNames
  {
    get
    {
      return _names;
    }
  }
}
