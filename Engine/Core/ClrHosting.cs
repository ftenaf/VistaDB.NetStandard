using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using VistaDB.Compatibility.SqlServer;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Functions;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core
{
  internal class ClrHosting : InsensitiveHashtable
  {
    private List<string> _sysProcs;

    internal ClrHosting()
    {
      this.EFInitialize();
    }

    internal ClrHosting.ClrProcedure this[string clrName]
    {
      get
      {
        return this.Contains((object) clrName) ? (ClrHosting.ClrProcedure) this[(object) clrName] : (ClrHosting.ClrProcedure) (object) null;
      }
    }

    internal void AddProcedure(string name, ClrHosting.ClrProcedure procedure)
    {
      this.Add((object) name, (object) procedure);
    }

    internal bool IsProcedureActive(string clrProcedure)
    {
      return this.Contains((object) clrProcedure);
    }

    internal bool IsAssemblyActive(string name)
    {
      foreach (ClrHosting.ClrProcedure clrProcedure in (Hashtable) this)
      {
        if (ClrHosting.EqualNames(name, clrProcedure.ParentAssembly.FullName))
          return true;
      }
      return false;
    }

    internal static byte[] COFFImage(string assemblyFileName)
    {
      using (FileStream fileStream = File.OpenRead(assemblyFileName))
      {
        byte[] buffer = new byte[fileStream.Length];
        fileStream.Read(buffer, 0, (int) fileStream.Length);
        return buffer;
      }
    }

    internal void Unregister(string procedureName)
    {
      this.Remove((object) procedureName);
    }

    internal static Assembly ActivateAssembly(byte[] assemblyBody, DirectConnection connection)
    {
      return Assembly.Load(assemblyBody);
    }

    internal static ClrHosting.ClrProcedure GetMethod(string clrHostedProcedure, Assembly assembly)
    {
      MethodInfo fillRowProcedure = (MethodInfo) null;
      Type[] types = assembly.GetTypes();
      MethodInfo procedure = ClrHosting.LookForMethod(clrHostedProcedure, types);
      if (procedure == null)
        throw new VistaDBException(380, clrHostedProcedure);
      foreach (VistaDBClrProcedureAttribute customAttribute in procedure.GetCustomAttributes(typeof (VistaDBClrProcedureAttribute), false))
      {
        string fillRow = customAttribute.FillRow;
        if (fillRow != null)
        {
          fillRowProcedure = ClrHosting.LookForMethod(fillRow, types);
          if (fillRowProcedure == null)
            throw new VistaDBException(384, fillRow);
          break;
        }
      }
      foreach (SqlFunctionAttribute customAttribute in procedure.GetCustomAttributes(typeof (SqlFunctionAttribute), false))
      {
        string fillRowMethodName = customAttribute.FillRowMethodName;
        if (!string.IsNullOrEmpty(fillRowMethodName))
        {
          fillRowProcedure = procedure.DeclaringType.GetMethod(fillRowMethodName);
          if (fillRowProcedure == null)
            throw new VistaDBException(384, fillRowMethodName);
          break;
        }
      }
      return new ClrHosting.ClrProcedure(clrHostedProcedure, assembly, procedure, fillRowProcedure);
    }

    private static MethodInfo LookForMethod(string clrHostedName, Type[] types)
    {
      foreach (Type type in types)
      {
        if (type.IsPublic)
        {
          foreach (MethodInfo method in type.GetMethods())
          {
            if (method.IsStatic && type.IsPublic && (method.MemberType == MemberTypes.Method && ClrHosting.EqualNames(type.FullName + "." + method.Name, clrHostedName)))
              return method;
          }
        }
      }
      return (MethodInfo) null;
    }

    internal static bool EqualNames(string a, string b)
    {
      return string.Compare(a, b, StringComparison.OrdinalIgnoreCase) == 0;
    }

    internal static Database.ClrProcedureCollection ListClrProcedures(string assemblyName, Assembly assembly)
    {
      Database.ClrProcedureCollection procedureCollection = new Database.ClrProcedureCollection();
      Type[] types = assembly.GetTypes();
      int num = 1;
      foreach (Type type in types)
      {
        if (type.IsPublic)
        {
          foreach (MethodInfo method in type.GetMethods())
          {
            if (method.IsStatic && type.IsPublic && method.MemberType == MemberTypes.Method)
            {
              string fullCLRProcedureName = type.FullName + "." + method.Name;
              procedureCollection.AddProcedure("Not assigned #" + num.ToString(), Database.ClrProcedureCollection.ClrProcedureInformation.CompileSignature(fullCLRProcedureName, method.ToString()), (string) null, Encoding.Unicode.GetBytes(assemblyName), false);
              ++num;
            }
          }
        }
      }
      foreach (Type type in types)
      {
        if (type.IsPublic)
        {
          foreach (MethodInfo method in type.GetMethods())
          {
            if (method.IsStatic && type.IsPublic && method.MemberType == MemberTypes.Method)
            {
              foreach (VistaDBClrProcedureAttribute customAttribute in method.GetCustomAttributes(typeof (VistaDBClrProcedureAttribute), false))
              {
                string fillRow = customAttribute.FillRow;
                if (fillRow != null)
                  procedureCollection.DropByFullName(fillRow);
              }
            }
          }
        }
      }
      return procedureCollection;
    }

    internal static Database.ClrTriggerCollection ListClrTriggers(string assemblyName, Assembly assembly)
    {
      Database.ClrTriggerCollection triggerCollection = new Database.ClrTriggerCollection();
      Type[] types = assembly.GetTypes();
      int num = 1;
      foreach (Type type in types)
      {
        if (type.IsPublic)
        {
          foreach (MethodInfo method in type.GetMethods())
          {
            if (method.IsStatic && type.IsPublic && (method.MemberType == MemberTypes.Method && method.GetParameters().Length <= 0))
            {
              string fullCLRProcedureName = type.FullName + "." + method.Name;
              triggerCollection.AddTrigger("Not assigned #" + num.ToString(), Database.ClrProcedureCollection.ClrProcedureInformation.CompileSignature(fullCLRProcedureName, method.ToString()), (string) null, Encoding.Unicode.GetBytes(assemblyName), false, Row.EmptyReference, 0L);
              ++num;
            }
          }
        }
      }
      foreach (Type type in types)
      {
        if (type.IsPublic)
        {
          foreach (MethodInfo method in type.GetMethods())
          {
            if (method.IsStatic && type.IsPublic && method.MemberType == MemberTypes.Method)
            {
              foreach (VistaDBClrProcedureAttribute customAttribute in method.GetCustomAttributes(typeof (VistaDBClrProcedureAttribute), false))
              {
                string fillRow = customAttribute.FillRow;
                if (fillRow != null)
                  triggerCollection.DropByFullName(fillRow);
              }
            }
          }
        }
      }
      return triggerCollection;
    }

    private void EFInitialize()
    {
      this._sysProcs = new List<string>();
      SystemFunctions.RegisterIntoHosting(this, (IList<string>) this._sysProcs);
    }

    internal IVistaDBClrProcedureCollection EnumSystemProcs()
    {
      Database.ClrProcedureCollection procedureCollection = new Database.ClrProcedureCollection();
      foreach (string sysProc in this._sysProcs)
      {
        ClrHosting.ClrProcedure clrProcedure = this[sysProc];
        procedureCollection.AddProcedure(sysProc, clrProcedure.Method.Name + (object) char.MinValue, (string) null, Encoding.Unicode.GetBytes(clrProcedure.ParentAssembly.FullName), true);
      }
      return (IVistaDBClrProcedureCollection) procedureCollection;
    }

    internal class ClrProcedure
    {
      private string procedureName;
      private Assembly assembly;
      private MethodInfo procedure;
      private MethodInfo fillRow;

      internal ClrProcedure(string name, Assembly assembly, MethodInfo procedure, MethodInfo fillRowProcedure)
      {
        this.procedureName = name;
        this.assembly = assembly;
        this.procedure = procedure;
        this.fillRow = fillRowProcedure;
      }

      internal Assembly ParentAssembly
      {
        get
        {
          return this.assembly;
        }
      }

      internal MethodInfo Method
      {
        get
        {
          return this.procedure;
        }
      }

      internal MethodInfo FillRowMethod
      {
        get
        {
          return this.fillRow;
        }
      }

      internal object Execute(params object[] prms)
      {
        try
        {
          ParameterInfo[] parameters = this.procedure.GetParameters();
          if (prms.Length != parameters.Length)
            throw new VistaDBException(382, this.procedure.ToString());
          return this.procedure.Invoke((object) null, prms);
        }
        catch
        {
          throw;
        }
      }

      internal object Execute()
      {
        return this.procedure.Invoke((object) null, (object[]) null);
      }

      internal object ExecFillRow(params object[] prms)
      {
        try
        {
          ParameterInfo[] parameters = this.fillRow.GetParameters();
          if (prms.Length != parameters.Length)
            throw new VistaDBException(382, this.fillRow.ToString());
          return this.fillRow.Invoke((object) null, prms);
        }
        catch (Exception ex)
        {
          throw;
        }
      }
    }
  }
}
