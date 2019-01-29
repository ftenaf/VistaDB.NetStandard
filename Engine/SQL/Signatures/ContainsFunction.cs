using System.Collections.Generic;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core.Indexing;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class ContainsFunction : Function
  {
    private const string SqlHint = "Example: SELECT * from TABLE WHERE CONTAINS( *, 'word')";
    private string pattern;
    private PatternFinder patternFinder;
    private bool prefixSearch;

    public ContainsFunction(SQLParser parser)
      : base(parser, -1, true)
    {
      this.dataType = VistaDBType.Bit;
      for (int index = 0; index < this.parameters.Count - 1; ++index)
        this.parameterTypes[index] = this.parameters[index].DataType;
      this.parameterTypes[this.parameters.Count - 1] = VistaDBType.NChar;
      this.optimizable = true;
      this.skipNull = false;
    }

    protected override void ParseParameters(SQLParser parser)
    {
      if (!parser.IsToken("("))
        throw new VistaDBSQLException(500, "\"(\" Example: SELECT * from TABLE WHERE CONTAINS( *, 'word')", this.lineNo, this.symbolNo);
      this.parameters = new List<Signature>();
      parser.SkipToken(true);
      this.parameters.Add(parser.NextSignature(false, true, 6));
      while (parser.IsToken(","))
        this.parameters.Add(parser.NextSignature(true, true, 6));
      parser.ExpectedExpression(")");
      this.CreatePattern(parser);
    }

    protected override object ExecuteSubProgram()
    {
      int index1 = 0;
      for (int index2 = this.parameters.Count - 1; index1 < index2; ++index1)
      {
        if (this.patternFinder.ContainsPattern((string) ((IValue) this.paramValues[index1]).Value, this.prefixSearch))
          return (object) true;
      }
      return (object) false;
    }

    private void FtsIndexExists()
    {
      IVistaDBIndexCollection indexes = this.parent.Database.TableSchema(this.parent.GetSourceTable(0).TableName).Indexes;
      IVistaDBKeyColumn[] vistaDbKeyColumnArray = (IVistaDBKeyColumn[]) null;
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
      SourceTable sourceTable = this.parent.GetSourceTable(0);
      IVistaDBIndexCollection indexes = this.parent.Database.TableSchema(sourceTable.TableName).Indexes;
      IVistaDBKeyColumn[] vistaDbKeyColumnArray = (IVistaDBKeyColumn[]) null;
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
      this.parameters.RemoveAt(0);
      sourceTable.Prepare();
      sourceTable.Open();
      foreach (IVistaDBKeyColumn vistaDbKeyColumn in vistaDbKeyColumnArray)
      {
        IColumn column = sourceTable.SimpleGetColumn(vistaDbKeyColumn.RowIndex);
        this.parameters.Insert(0, (Signature) new ColumnSignature(sourceTable, column.RowIndex, this.parent));
        this.paramValues = new IColumn[this.parameters.Count - 1];
      }
    }

    protected override bool OnOptimize(ConstraintOperations constrainOperations)
    {
      if (this.pattern == null && this.parameters.Count > 0)
        this.CreatePattern((SQLParser) null);
      if (this.IsStopWord(this.pattern))
        return false;
      if ((this.parameters[0] as ColumnSignature).SignatureType == SignatureType.MultiplyColumn)
        this.PrepareMultiplyColumn();
      else
        this.FtsIndexExists();
      int index1 = this.parameters.Count - 1;
      bool flag;
      if (this.prefixSearch)
      {
        Signature low;
        Signature high;
        this.patternFinder.GetOptimizationScopeSignatures(this.parent, out low, out high);
        flag = constrainOperations.AddLogicalBetween((ColumnSignature) this.parameters[0], low, high, true);
        for (int index2 = 1; flag && index2 < index1; ++index2)
          flag = constrainOperations.AddLogicalBetween((ColumnSignature) this.parameters[index2], low, high, true) && constrainOperations.AddLogicalOr();
      }
      else
      {
        Signature parameter = this.parameters[index1];
        flag = constrainOperations.AddLogicalCompare(this.parameters[0], parameter, CompareOperation.Equal, CompareOperation.Equal, true);
        for (int index2 = 1; flag && index2 < index1; ++index2)
          flag = constrainOperations.AddLogicalCompare(this.parameters[index2], parameter, CompareOperation.Equal, CompareOperation.Equal, true) && constrainOperations.AddLogicalOr();
      }
      return flag;
    }

    public override void SetChanged()
    {
      this.pattern = (string) null;
      base.SetChanged();
    }

    private bool IsStopWord(string word)
    {
      return FTSIndex.WordBreaker.IsStopWord(word);
    }

    private void CreatePattern(SQLParser parser)
    {
      Signature parameter = this.parameters[this.parameters.Count - 1];
      if (parser != null && parameter.SignatureType == SignatureType.Parameter)
        return;
      this.pattern = ((IValue) parameter.Execute()).Value as string;
      int num1 = this.pattern.IndexOf('"');
      int num2 = this.pattern.IndexOf("*");
      if (this.pattern.IndexOf('%') >= 0 || num1 == -1 && num2 >= 0)
        throw new VistaDBSQLException(634, "CONTAINS should not include percent. Try Contains( *, '\"word*\"') instead. Example: SELECT * from TABLE WHERE CONTAINS( *, 'word')", this.lineNo, this.symbolNo);
      if (this.pattern.IndexOf(' ') >= 0)
      {
        string upperInvariant = this.pattern.ToUpperInvariant();
        char[] chArray = new char[1]{ ' ' };
        foreach (string str in upperInvariant.Split(chArray))
        {
          if (str.IndexOf("AND") >= 0 || str.IndexOf("OR") >= 0 || str.IndexOf("NOT") >= 0)
            throw new VistaDBSQLException(634, "CONTAINS not built correctly. Only single word or start of word pattern matches are supported at this time. Example: SELECT * from TABLE WHERE CONTAINS( *, 'word')", this.lineNo, this.symbolNo);
        }
      }
      if (num1 > -1)
      {
        this.pattern = this.pattern.Trim();
        if (this.pattern.IndexOf('"') > 0 || this.pattern[this.pattern.Length - 1] != '"')
          throw new VistaDBSQLException(634, "Example: SELECT * from TABLE WHERE CONTAINS( *, 'word')", this.lineNo, this.symbolNo);
        this.pattern = this.pattern.Substring(1, num2 < 1 ? this.pattern.Length - 2 : num2 - 1);
        if (parser != null)
          this.parameters[this.parameters.Count - 1] = (Signature) ConstantSignature.CreateSignature(this.pattern, VistaDBType.NChar, parser);
        this.prefixSearch = num2 > -1;
      }
      this.patternFinder = new PatternFinder(this.pattern, this.parent.Connection);
    }
  }
}
