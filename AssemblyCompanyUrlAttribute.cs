using System;
using System.Reflection;

[Obfuscation(ApplyToMembers = true, Exclude = true)]
[AttributeUsage(AttributeTargets.Assembly, AllowMultiple = false)]
internal sealed class AssemblyCompanyUrlAttribute : Attribute
{
  private string _url;

  public AssemblyCompanyUrlAttribute(string url)
  {
    this._url = url;
  }

  public string CompanyUrl
  {
    get
    {
      return this._url;
    }
  }
}
