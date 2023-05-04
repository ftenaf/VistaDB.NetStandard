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
            EFInitialize();
        }

        internal ClrProcedure this[string clrName]
        {
            get
            {
                return Contains((object)clrName) ? (ClrProcedure)this[(object)clrName] : (ClrProcedure)(object)null;
            }
        }

        internal void AddProcedure(string name, ClrProcedure procedure)
        {
            Add((object)name, (object)procedure);
        }

        internal bool IsProcedureActive(string clrProcedure)
        {
            return Contains((object)clrProcedure);
        }

        internal bool IsAssemblyActive(string name)
        {
            foreach (ClrProcedure clrProcedure in (Hashtable)this)
            {
                if (EqualNames(name, clrProcedure.ParentAssembly.FullName))
                    return true;
            }
            return false;
        }

        internal static byte[] COFFImage(string assemblyFileName)
        {
            using (FileStream fileStream = File.OpenRead(assemblyFileName))
            {
                byte[] buffer = new byte[fileStream.Length];
                fileStream.Read(buffer, 0, (int)fileStream.Length);
                return buffer;
            }
        }

        internal void Unregister(string procedureName)
        {
            Remove((object)procedureName);
        }

        internal static Assembly ActivateAssembly(byte[] assemblyBody, DirectConnection connection)
        {
            return Assembly.Load(assemblyBody);
        }

        internal static ClrProcedure GetMethod(string clrHostedProcedure, Assembly assembly)
        {
            MethodInfo fillRowProcedure = (MethodInfo)null;
            Type[] types = assembly.GetTypes();
            MethodInfo procedure = LookForMethod(clrHostedProcedure, types);
            if (procedure == null)
                throw new VistaDBException(380, clrHostedProcedure);
            foreach (VistaDBClrProcedureAttribute customAttribute in procedure.GetCustomAttributes(typeof(VistaDBClrProcedureAttribute), false))
            {
                string fillRow = customAttribute.FillRow;
                if (fillRow != null)
                {
                    fillRowProcedure = LookForMethod(fillRow, types);
                    if (fillRowProcedure == null)
                        throw new VistaDBException(384, fillRow);
                    break;
                }
            }
            foreach (SqlFunctionAttribute customAttribute in procedure.GetCustomAttributes(typeof(SqlFunctionAttribute), false))
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
            return new ClrProcedure(clrHostedProcedure, assembly, procedure, fillRowProcedure);
        }

        private static MethodInfo LookForMethod(string clrHostedName, Type[] types)
        {
            foreach (Type type in types)
            {
                if (type.IsPublic)
                {
                    foreach (MethodInfo method in type.GetMethods())
                    {
                        if (method.IsStatic && type.IsPublic && (method.MemberType == MemberTypes.Method && EqualNames(type.FullName + "." + method.Name, clrHostedName)))
                            return method;
                    }
                }
            }
            return (MethodInfo)null;
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
                            procedureCollection.AddProcedure("Not assigned #" + num.ToString(), Database.ClrProcedureCollection.ClrProcedureInformation.CompileSignature(fullCLRProcedureName, method.ToString()), (string)null, Encoding.Unicode.GetBytes(assemblyName), false);
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
                            foreach (VistaDBClrProcedureAttribute customAttribute in method.GetCustomAttributes(typeof(VistaDBClrProcedureAttribute), false))
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
                            triggerCollection.AddTrigger("Not assigned #" + num.ToString(), Database.ClrProcedureCollection.ClrProcedureInformation.CompileSignature(fullCLRProcedureName, method.ToString()), (string)null, Encoding.Unicode.GetBytes(assemblyName), false, Row.EmptyReference, 0L);
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
                            foreach (VistaDBClrProcedureAttribute customAttribute in method.GetCustomAttributes(typeof(VistaDBClrProcedureAttribute), false))
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
            _sysProcs = new List<string>();
            SystemFunctions.RegisterIntoHosting(this, (IList<string>)_sysProcs);
        }

        internal IVistaDBClrProcedureCollection EnumSystemProcs()
        {
            Database.ClrProcedureCollection procedureCollection = new Database.ClrProcedureCollection();
            foreach (string sysProc in _sysProcs)
            {
                ClrProcedure clrProcedure = this[sysProc];
                procedureCollection.AddProcedure(sysProc, clrProcedure.Method.Name + (object)char.MinValue, (string)null, Encoding.Unicode.GetBytes(clrProcedure.ParentAssembly.FullName), true);
            }
            return (IVistaDBClrProcedureCollection)procedureCollection;
        }

        internal class ClrProcedure
        {
            private string procedureName;
            private Assembly assembly;
            private MethodInfo procedure;
            private MethodInfo fillRow;

            internal ClrProcedure(string name, Assembly assembly, MethodInfo procedure, MethodInfo fillRowProcedure)
            {
                procedureName = name;
                this.assembly = assembly;
                this.procedure = procedure;
                fillRow = fillRowProcedure;
            }

            internal Assembly ParentAssembly
            {
                get
                {
                    return assembly;
                }
            }

            internal MethodInfo Method
            {
                get
                {
                    return procedure;
                }
            }

            internal MethodInfo FillRowMethod
            {
                get
                {
                    return fillRow;
                }
            }

            internal object Execute(params object[] prms)
            {
                try
                {
                    ParameterInfo[] parameters = procedure.GetParameters();
                    if (prms.Length != parameters.Length)
                        throw new VistaDBException(382, procedure.ToString());
                    return procedure.Invoke((object)null, prms);
                }
                catch
                {
                    throw;
                }
            }

            internal object Execute()
            {
                return procedure.Invoke((object)null, (object[])null);
            }

            internal object ExecFillRow(params object[] prms)
            {
                try
                {
                    ParameterInfo[] parameters = fillRow.GetParameters();
                    if (prms.Length != parameters.Length)
                        throw new VistaDBException(382, fillRow.ToString());
                    return fillRow.Invoke((object)null, prms);
                }
                catch (Exception)
                {
                    throw;
                }
            }
        }
    }
}
