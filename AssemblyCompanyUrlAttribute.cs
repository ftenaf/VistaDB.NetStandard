using System;
using System.Reflection;

[Obfuscation(ApplyToMembers = true, Exclude = true)]
[AttributeUsage(AttributeTargets.Assembly, AllowMultiple = false)]
internal sealed class AssemblyCompanyUrlAttribute : Attribute
{
  private string _url;

  public AssemblyCompanyUrlAttribute(string url)
  {
    _url = url;
  }

  public string CompanyUrl
  {
    get
    {
      return _url;
    }
  }
}
