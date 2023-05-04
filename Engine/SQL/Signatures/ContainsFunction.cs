using System.Collections.Generic;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core.Indexing;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class ContainsFunction : Function
  {
        private string pattern;
    private PatternFinder patternFinder;
    private bool prefixSearch;

    public ContainsFunction(SQLParser parser)
      : base(parser, -1, true)
    {
      dataType = VistaDBType.Bit;
      for (int index = 0; index < parameters.Count - 1; ++index)
        parameterTypes[index] = parameters[index].DataType;
      parameterTypes[parameters.Count - 1] = VistaDBType.NChar;
      optimizable = true;
      skipNull = false;
    }

    protected override void ParseParameters(SQLParser parser)
    {
      if (!parser.IsToken("("))
        throw new VistaDBSQLException(500, "\"(\" Example: SELECT * from TABLE WHERE CONTAINS( *, 'word')", lineNo, symbolNo);
      parameters = new List<Signature>();
      parser.SkipToken(true);
      parameters.Add(parser.NextSignature(false, true, 6));
      while (parser.IsToken(","))
        parameters.Add(parser.NextSignature(true, true, 6));
      parser.ExpectedExpression(")");
      CreatePattern(parser);
    }

    protected override object ExecuteSubProgram()
    {
      int index1 = 0;
      for (int index2 = parameters.Count - 1; index1 < index2; ++index1)
      {
        if (patternFinder.ContainsPattern((string)paramValues[index1].Value, prefixSearch))
          return true;
      }
      return false;
    }

    private void FtsIndexExists()
    {
      IVistaDBIndexCollection indexes = parent.Database.TableSchema(parent.GetSourceTable(0).TableName).Indexes;
      IVistaDBKeyColumn[] vistaDbKeyColumnArray = null;
      foreach (IVistaDBIndexInformation indexInformation in (IEnumerable<IVistaDBIndexInformation>) indexes.Values)
      {
        if (indexInformation.FullTextSearch)
        {
          vistaDbKeyColumnArray = indexInformation.KeyStructure;
          break;
        }
      }
      if (vistaDbKeyColumnArray == null)
        throw new VistaDBException(636, "Example: create fulltext index IndexName on Table(column1, column2)");
    }

    private void PrepareMultiplyColumn()
    {
      SourceTable sourceTable = parent.GetSourceTable(0);
      IVistaDBIndexCollection indexes = parent.Database.TableSchema(sourceTable.TableName).Indexes;
      IVistaDBKeyColumn[] vistaDbKeyColumnArray = null;
      foreach (IVistaDBIndexInformation indexInformation in (IEnumerable<IVistaDBIndexInformation>) indexes.Values)
      {
        if (indexInformation.FullTextSearch)
        {
          vistaDbKeyColumnArray = indexInformation.KeyStructure;
          break;
        }
      }
      if (vistaDbKeyColumnArray == null)
        throw new VistaDBException(636, "Example: create fulltext index IndexName on Table(textcolumn1, ntextcolumn2)");
      parameters.RemoveAt(0);
      sourceTable.Prepare();
      sourceTable.Open();
      foreach (IVistaDBKeyColumn vistaDbKeyColumn in vistaDbKeyColumnArray)
      {
        IColumn column = sourceTable.SimpleGetColumn(vistaDbKeyColumn.RowIndex);
        parameters.Insert(0, new ColumnSignature(sourceTable, column.RowIndex, parent));
        paramValues = new IColumn[parameters.Count - 1];
      }
    }

    protected override bool OnOptimize(ConstraintOperations constrainOperations)
    {
      if (pattern == null && parameters.Count > 0)
        CreatePattern(null);
      if (IsStopWord(pattern))
        return false;
      if ((parameters[0] as ColumnSignature).SignatureType == SignatureType.MultiplyColumn)
        PrepareMultiplyColumn();
      else
        FtsIndexExists();
      int index1 = parameters.Count - 1;
      bool flag;
      if (prefixSearch)
      {
        Signature low;
        Signature high;
        patternFinder.GetOptimizationScopeSignatures(parent, out low, out high);
        flag = constrainOperations.AddLogicalBetween((ColumnSignature) parameters[0], low, high, true);
        for (int index2 = 1; flag && index2 < index1; ++index2)
          flag = constrainOperations.AddLogicalBetween((ColumnSignature) parameters[index2], low, high, true) && constrainOperations.AddLogicalOr();
      }
      else
      {
        Signature parameter = parameters[index1];
        flag = constrainOperations.AddLogicalCompare(parameters[0], parameter, CompareOperation.Equal, CompareOperation.Equal, true);
        for (int index2 = 1; flag && index2 < index1; ++index2)
          flag = constrainOperations.AddLogicalCompare(parameters[index2], parameter, CompareOperation.Equal, CompareOperation.Equal, true) && constrainOperations.AddLogicalOr();
      }
      return flag;
    }

    public override void SetChanged()
    {
      pattern = null;
      base.SetChanged();
    }

    private bool IsStopWord(string word)
    {
      return FTSIndex.WordBreaker.IsStopWord(word);
    }

    private void CreatePattern(SQLParser parser)
    {
      Signature parameter = parameters[parameters.Count - 1];
      if (parser != null && parameter.SignatureType == SignatureType.Parameter)
        return;
      pattern = parameter.Execute().Value as string;
      int num1 = pattern.IndexOf('"');
      int num2 = pattern.IndexOf("*");
      if (pattern.IndexOf('%') >= 0 || num1 == -1 && num2 >= 0)
        throw new VistaDBSQLException(634, "CONTAINS should not include percent. Try Contains( *, '\"word*\"') instead. Example: SELECT * from TABLE WHERE CONTAINS( *, 'word')", lineNo, symbolNo);
      if (pattern.IndexOf(' ') >= 0)
      {
        string upperInvariant = pattern.ToUpperInvariant();
        char[] chArray = new char[1]{ ' ' };
        foreach (string str in upperInvariant.Split(chArray))
        {
          if (str.IndexOf("AND") >= 0 || str.IndexOf("OR") >= 0 || str.IndexOf("NOT") >= 0)
            throw new VistaDBSQLException(634, "CONTAINS not built correctly. Only single word or start of word pattern matches are supported at this time. Example: SELECT * from TABLE WHERE CONTAINS( *, 'word')", lineNo, symbolNo);
        }
      }
      if (num1 > -1)
      {
        pattern = pattern.Trim();
        if (pattern.IndexOf('"') > 0 || pattern[pattern.Length - 1] != '"')
          throw new VistaDBSQLException(634, "Example: SELECT * from TABLE WHERE CONTAINS( *, 'word')", lineNo, symbolNo);
        pattern = pattern.Substring(1, num2 < 1 ? pattern.Length - 2 : num2 - 1);
        if (parser != null)
          parameters[parameters.Count - 1] = ConstantSignature.CreateSignature(pattern, VistaDBType.NChar, parser);
        prefixSearch = num2 > -1;
      }
      patternFinder = new PatternFinder(pattern, parent.Connection);
    }
  }
}
