using System;
using System.Reflection;

[Obfuscation(ApplyToMembers = true, Exclude = true)]
[AttributeUsage(AttributeTargets.Assembly, AllowMultiple = true)]
internal sealed class FactoryClassAttribute : Attribute
{
  private string _className;
  private string _name;
  private string[] _names;

  public FactoryClassAttribute(Type classType)
  {
    this._className = classType.FullName;
    this._name = classType.Name;
    this._names = new string[0];
  }

  public FactoryClassAttribute(Type classType, string name)
  {
    this._className = classType.FullName;
    this._name = name;
    this._names = new string[0];
  }

  public FactoryClassAttribute(Type classType, string name, params string[] alternateNames)
  {
    this._className = classType.FullName;
    this._name = name;
    this._names = alternateNames;
  }

  public string ClassName
  {
    get
    {
      return this._className;
    }
  }

  public string Name
  {
    get
    {
      return this._name;
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
