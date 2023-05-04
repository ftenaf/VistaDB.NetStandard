using System;
using System.Collections.Generic;
using System.Globalization;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class Signature
  {
    private static readonly int MinNameLength = 4;
    private int entry = -1;
    private char[] name;
    private int nameLen;
    private int groupId;
    private Operations operation;
    private VistaDBType returnType;
    private Priorities priority;
    private Parameters parameters;
    private char[] bgnOfGroup;
    private char[] endOfGroup;
    private char[] delimiter;
    private char spaceChar;
    private int endOfGroupEntry;
    private readonly int bypassToGroup;
    private int numberOperands;
    protected int unaryOffset;
    protected bool allowUnaryToFollow;

    protected Signature(string name, int groupId, Operations operation, Priorities priority, VistaDBType returnType, string bgnOfGroup, string delimiter, char spaceChar, string endOfGroup, int endOfGroupEntry)
    {
      SetName(name);
      this.groupId = groupId;
      this.operation = operation;
      this.returnType = returnType;
      this.spaceChar = spaceChar;
      this.priority = priority;
      parameters = new Parameters();
      SetBeginOfGroup(bgnOfGroup);
      SetEndOfGroup(endOfGroup);
      SetDelimiter(delimiter);
      this.endOfGroupEntry = endOfGroupEntry;
    }

    protected Signature(string name, int groupId, Operations operation, Priorities priority, VistaDBType returnType)
      : this(name, groupId, operation, priority, returnType, "(", ",", ' ', ")", 0)
    {
    }

    protected Signature(string name, int groupId, Operations operation, Priorities priority, VistaDBType returnType, int endOfGroupId)
      : this(name, groupId, operation, priority, returnType, "(", ",", ' ', ")", endOfGroupId)
    {
    }

    protected Signature(string name, int groupId, Operations operation, Priorities priority, VistaDBType returnType, string bgnOfGroup, string delimiter, char spaceChar, string endOfGroup)
      : this(name, groupId, operation, priority, returnType, bgnOfGroup, delimiter, spaceChar, endOfGroup, 0)
    {
    }

    private void SetBeginOfGroup(string bgnOfGroup)
    {
      if (bgnOfGroup == null)
      {
        this.bgnOfGroup = (char[]) null;
      }
      else
      {
        this.bgnOfGroup = new char[bgnOfGroup.Length];
        bgnOfGroup.CopyTo(0, this.bgnOfGroup, 0, bgnOfGroup.Length);
      }
    }

    private void SetEndOfGroup(string endOfGroup)
    {
      if (endOfGroup == null)
      {
        this.endOfGroup = (char[]) null;
      }
      else
      {
        this.endOfGroup = new char[endOfGroup.Length];
        endOfGroup.CopyTo(0, this.endOfGroup, 0, endOfGroup.Length);
      }
    }

    private void SetDelimiter(string delimiter)
    {
      if (endOfGroup == null)
      {
        endOfGroup = (char[]) null;
      }
      else
      {
        this.delimiter = new char[delimiter.Length];
        delimiter.CopyTo(0, this.delimiter, 0, delimiter.Length);
      }
    }

    internal char[] BgnOfGroup
    {
      get
      {
        return bgnOfGroup;
      }
    }

    internal char[] EndOfGroup
    {
      get
      {
        return endOfGroup;
      }
    }

    internal int EndOfGroupEntry
    {
      get
      {
        return endOfGroupEntry;
      }
    }

    internal char[] Delimiter
    {
      get
      {
        return delimiter;
      }
    }

    internal char SpaceChar
    {
      get
      {
        return spaceChar;
      }
    }

    internal int NumberFormalOperands
    {
      get
      {
        return numberOperands;
      }
    }

    internal int BypassToGroup
    {
      get
      {
        return bypassToGroup;
      }
    }

    internal int Group
    {
      get
      {
        return groupId;
      }
    }

    internal int Entry
    {
      get
      {
        return entry;
      }
      set
      {
        entry = value;
      }
    }

    internal int UnaryEntry
    {
      get
      {
        return entry + unaryOffset;
      }
    }

    internal Operations Operation
    {
      get
      {
        return operation;
      }
    }

    internal char[] Name
    {
      get
      {
        return name;
      }
    }

    internal int NameLen
    {
      get
      {
        return nameLen;
      }
    }

    internal Priorities Priority
    {
      get
      {
        return priority;
      }
    }

    internal VistaDBType ReturnType
    {
      get
      {
        return returnType;
      }
    }

    internal bool UnaryOverloading
    {
      get
      {
        return unaryOffset > 0;
      }
    }

    internal bool AllowUnaryToFollow
    {
      get
      {
        return allowUnaryToFollow;
      }
    }

    protected virtual int NumberActiveOperands
    {
      get
      {
        return NumberFormalOperands;
      }
    }

    protected virtual bool OnCompatibleType(int parameterIndex, VistaDBType actualType)
    {
      VistaDBType parameter = parameters[parameterIndex];
      if (parameter != actualType)
        return parameter == VistaDBType.Unknown;
      return true;
    }

    protected virtual void OnFixReturnTypeAndParameters(Collector collector, int offset, VistaDBType newType)
    {
      returnType = newType;
    }

    protected virtual void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
    }

    internal bool CompatibleType(int parameterIndex, VistaDBType actualType)
    {
      return OnCompatibleType(parameterIndex, actualType);
    }

    internal void FixReturnTypeAndParameters(Collector collector, int offset)
    {
      OnFixReturnTypeAndParameters(collector, offset, ReturnType);
    }

    internal VistaDBType GetParameterType(int index)
    {
      return parameters[index];
    }

    internal void SetParameterType(int index, VistaDBType type)
    {
      if (index >= 0)
        parameters[index] = type;
      else
        returnType = type;
    }

    internal void SetName(string name)
    {
      if (name == null)
      {
        this.name = (char[]) null;
        nameLen = 0;
      }
      else
      {
        this.name = new char[name.Length];
        name.CopyTo(0, this.name, 0, name.Length);
        nameLen = this.name.Length;
      }
    }

    internal void AddParameter(VistaDBType type)
    {
      numberOperands = parameters.AddParameter(type);
    }

    internal int IncludedName(char[] buffer, int offset, CultureInfo culture)
    {
      if (buffer == null)
        return 0;
      int index = 0;
      while (index < nameLen && index + offset < buffer.Length && char.ToUpper(buffer[index + offset], culture).Equals(name[index]))
        ++index;
      if (index != nameLen && (index < MinNameLength || index > nameLen))
        return 0;
      return index;
    }

    internal bool IsSameName(char[] buffer, int offset, CultureInfo culture)
    {
      if (nameLen > 0)
        return IncludedName(buffer, offset, culture) == nameLen;
      return false;
    }

    internal void Execute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      try
      {
        OnExecute(pcode, entry, connection, contextStorage, contextRow, ref bypassNextGroup, rowResult);
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 292, pcode.ToString());
      }
    }

    internal virtual Signature DoCloneSignature()
    {
      return this;
    }

    internal enum Operations
    {
      BgnGroup,
      EndGroup,
      Delimiter,
      Null,
      False,
      True,
      And,
      Or,
      Xor,
      Not,
      Equivalence,
      GreatEqual,
      Great,
      LessEqual,
      Less,
      IsNull,
      Nomark,
    }

    internal enum Priorities
    {
      Endline,
      MinPriority,
      Setting,
      IsBitwise,
      Bitwise,
      Denial,
      Comparing,
      Summation,
      Mutliplication,
      PowerRaising,
      UnarySummation,
      StdOperator,
      Generator,
      MaximumPriority,
    }

    private class Parameters : List<VistaDBType>
    {
      internal int AddParameter(VistaDBType type)
      {
        Add(type);
        return Count;
      }
    }
  }
}
