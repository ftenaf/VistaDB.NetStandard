using System.Runtime.InteropServices;

namespace VistaDB.Engine.Core
{
  [StructLayout(LayoutKind.Sequential, Size = 1)]
  internal struct XmlSchema
  {
    internal const string NameSpace = "VistaDB";
    internal const string Concatenation = ":";
    internal const string Sensitivity = "sensitivity";
    internal const string Specification = "specification";
    internal const string DbObjects = "databaseObjects";

    [StructLayout(LayoutKind.Sequential, Size = 1)]
    internal struct Table
    {
      internal const string Description = "description";
      internal const string TableType = "tableType";
      internal const string Indexes = "indexes";
      internal const int IndecesIndex = 0;
      internal const string Identities = "identities";
      internal const int IdentitiesIndex = 0;
      internal const string Constraints = "constraints";
      internal const int ConstraintsIndex = 0;
      internal const string Defaults = "defaultValues";
      internal const int DefaultsIndex = 0;

      [StructLayout(LayoutKind.Sequential, Size = 1)]
      internal struct Column
      {
        internal const string VdbType = "vdbType";
        internal const string Encrypted = "encrypted";
        internal const string Packed = "packed";
        internal const string CodePage = "codepage";
        internal const string Description = "description";
        internal const string SyncService = "sync";
      }

      [StructLayout(LayoutKind.Sequential, Size = 1)]
      internal struct Index
      {
        internal const string KeyExpression = "key";
        internal const string Unique = "unique";
        internal const string Fts = "fts";
      }

      [StructLayout(LayoutKind.Sequential, Size = 1)]
      internal struct Identity
      {
        internal const string Seed = "seedExpression";
        internal const string Step = "stepExpression";
      }

      [StructLayout(LayoutKind.Sequential, Size = 1)]
      internal struct Constraint
      {
        internal const string Expression = "expression";
        internal const string Description = "description";
        internal const string UseInInsert = "insertion";
        internal const string UseInUpdate = "update";
        internal const string UseInDelete = "delete";
      }

      [StructLayout(LayoutKind.Sequential, Size = 1)]
      internal struct DefaultValue
      {
        internal const string Expression = "expression";
        internal const string Description = "description";
        internal const string UseInUpdate = "update";
      }
    }

    [StructLayout(LayoutKind.Sequential, Size = 1)]
    internal struct Relation
    {
      internal const string Description = "description";
    }

    [StructLayout(LayoutKind.Sequential, Size = 1)]
    internal struct DatabaseObjects
    {
      internal const string ViewList = "views";
      internal const string AssemblyList = "assemblies";
      internal const string CLRProcs = "clrProcs";

      [StructLayout(LayoutKind.Sequential, Size = 1)]
      internal struct View
      {
        internal const string Name = "name";
        internal const int NameIndex = 0;
        internal const string Expression = "expression";
        internal const int ExpresionIndex = 1;
      }

      [StructLayout(LayoutKind.Sequential, Size = 1)]
      internal struct Assembly
      {
        internal const string Name = "name";
        internal const int NameIndex = 0;
        internal const string Fullname = "fullName";
        internal const int FullNameIndex = 1;
        internal const string RuntimeVersion = "runtimeVersion";
        internal const int VersionIndex = 2;
        internal const string Description = "description";
        internal const int DescriptionIndex = 3;
        internal const string CoffImage = "coffImage";
        internal const int ImageIndex = 4;
        internal const string VistaDBVersion = "vistadbVersion";
        internal const int VdbVersionIndex = 5;
      }

      [StructLayout(LayoutKind.Sequential, Size = 1)]
      internal struct CLRPRoc
      {
        internal const string Name = "name";
        internal const int NameIndex = 0;
        internal const string FullCLRName = "fullClrName";
        internal const int FullCLRNameIndex = 1;
        internal const string AssemblyName = "assemblyName";
        internal const int AssemblyNameIndex = 2;
        internal const string Signature = "signature";
        internal const int SignatureIndex = 3;
        internal const string Description = "description";
        internal const int DescriptionIndex = 4;
      }
    }
  }
}
