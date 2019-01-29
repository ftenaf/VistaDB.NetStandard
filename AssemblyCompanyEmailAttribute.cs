using System;
using System.Reflection;

[AttributeUsage(AttributeTargets.Assembly, AllowMultiple = false)]
[Obfuscation(ApplyToMembers = true, Exclude = true)]
internal sealed class AssemblyCompanyEmailAttribute : Attribute
{
  private string _email;

  public AssemblyCompanyEmailAttribute(string email)
  {
    this._email = email;
  }

  public string CompanyEmail
  {
    get
    {
      return this._email;
    }
  }
}
