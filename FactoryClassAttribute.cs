﻿using System;
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
    _className = classType.FullName;
    _name = classType.Name;
    _names = new string[0];
  }

  public FactoryClassAttribute(Type classType, string name)
  {
    _className = classType.FullName;
    _name = name;
    _names = new string[0];
  }

  public FactoryClassAttribute(Type classType, string name, params string[] alternateNames)
  {
    _className = classType.FullName;
    _name = name;
    _names = alternateNames;
  }

  public string ClassName
  {
    get
    {
      return _className;
    }
  }

  public string Name
  {
    get
    {
      return _name;
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
