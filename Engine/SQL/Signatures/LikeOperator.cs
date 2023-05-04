using System.Collections.Generic;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class LikeOperator : Signature
  {
    protected Signature expression;
    private Signature pattern;
    private string escapeCharacter;
    protected PatternFinder finder;
    private IColumn exprResult;

    public LikeOperator(Signature expression, SQLParser parser)
      : base(parser)
    {
      signatureType = SignatureType.Expression;
      dataType = VistaDBType.Bit;
      this.expression = expression;
      finder = null;
      exprResult = null;
      pattern = parser.NextSignature(true, true, 2);
      if (parser.IsToken("ESCAPE"))
      {
        parser.SkipToken(true);
        if (parser.TokenValue.TokenType != TokenType.String)
          throw new VistaDBSQLException(553, "", lineNo, symbolNo);
        escapeCharacter = parser.TokenValue.Token;
        if (escapeCharacter.Length != 1)
          throw new VistaDBSQLException(553, "", lineNo, symbolNo);
        parser.SkipToken(false);
      }
      else
        escapeCharacter = null;
    }

    public override SignatureType OnPrepare()
    {
      expression = ConstantSignature.PrepareAndCheckConstant(expression, VistaDBType.NChar);
      if (!Utils.CompatibleTypes(expression.DataType, VistaDBType.NChar))
        throw new VistaDBSQLException(559, "", lineNo, symbolNo);
      if (!Utils.IsCharacterDataType(expression.DataType))
        exprResult = CreateColumn(VistaDBType.NChar);
      pattern = ConstantSignature.PrepareAndCheckConstant(pattern, VistaDBType.NChar);
      if (!Utils.CompatibleTypes(pattern.DataType, VistaDBType.NChar))
        throw new VistaDBSQLException(552, "", pattern.LineNo, pattern.SymbolNo);
      CalcOptimizeLevel();
      if (expression.SignatureType == SignatureType.Constant && pattern.SignatureType == SignatureType.Constant || (expression.AlwaysNull || pattern.AlwaysNull))
        return SignatureType.Constant;
      return signatureType;
    }

    protected override bool OnOptimize(ConstraintOperations constrainOperations)
    {
      int chunkCount;
      if (!CreatePattern() || finder.GetOptimizationLevel(out chunkCount) != OptimizationLevel.Full)
        return false;
      Signature low;
      Signature high;
      finder.GetOptimizationScopeSignatures(parent, out low, out high);
      if (chunkCount > 1)
        return constrainOperations.AddLogicalBetween((ColumnSignature) expression, low, high, false);
      return constrainOperations.AddLogicalCompare(expression, low, CompareOperation.Equal, CompareOperation.Equal, false);
    }

    protected override IColumn InternalExecute()
    {
      if (!AlwaysNull && GetIsChanged())
      {
        IColumn column = expression.Execute();
        if (!CreatePattern())
        {
                    result.Value = false;
        }
        else
        {
          string matchExpr;
          if (exprResult != null)
          {
            Convert(column, exprResult);
            matchExpr = (string)exprResult.Value;
          }
          else
            matchExpr = (string)column.Value;
			result.Value = column.IsNull ? false : (Compare(matchExpr) ? true : false);
        }
      }
      return result;
    }

    protected virtual bool Compare(string matchExpr)
    {
      return finder.CompareWithRegEx(matchExpr) > 0;
    }

    protected override bool IsEquals(Signature signature)
    {
      if (signature.GetType() != GetType())
        return false;
      LikeOperator likeOperator = (LikeOperator) signature;
      if (expression == likeOperator.expression && pattern == likeOperator.pattern)
        return parent.Connection.CompareString(escapeCharacter, likeOperator.escapeCharacter, true) == 0;
      return false;
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
      expression = expression.Relink(signature, ref columnCount);
      pattern = expression.Relink(signature, ref columnCount);
    }

    public override void SetChanged()
    {
      expression.SetChanged();
      pattern.SetChanged();
    }

    public override void ClearChanged()
    {
      expression.ClearChanged();
      pattern.ClearChanged();
    }

    protected override bool InternalGetIsChanged()
    {
      if (!expression.GetIsChanged())
        return pattern.GetIsChanged();
      return true;
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
      expression.GetAggregateFunctions(list);
      pattern.GetAggregateFunctions(list);
    }

    public override int ColumnCount
    {
      get
      {
        return expression.ColumnCount + pattern.ColumnCount;
      }
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      bool distinct1;
      bool distinct2;
      bool flag = expression.HasAggregateFunction(out distinct1) | expression.HasAggregateFunction(out distinct2);
      distinct = distinct1 || distinct2;
      return flag;
    }

    public override bool AlwaysNull
    {
      get
      {
        if (!expression.AlwaysNull)
          return pattern.AlwaysNull;
        return true;
      }
    }

    public override bool IsNull
    {
      get
      {
        if (!expression.IsNull)
          return pattern.IsNull;
        return true;
      }
    }

    private void CalcOptimizeLevel()
    {
      if (expression.SignatureType != SignatureType.Column)
      {
        optimizable = false;
      }
      else
      {
        switch (pattern.SignatureType)
        {
          case SignatureType.Constant:
          case SignatureType.Parameter:
          case SignatureType.ExternalColumn:
            optimizable = true;
            break;
          default:
            optimizable = false;
            break;
        }
      }
    }

    protected bool CreatePattern()
    {
      if (!this.pattern.GetIsChanged())
        return finder != null;
      this.pattern.Execute();
      if (this.pattern.Result.IsNull)
        return false;
      string pattern;
      if (Utils.IsCharacterDataType(this.pattern.DataType))
      {
        pattern = (string)this.pattern.Execute().Value;
      }
      else
      {
        IColumn column = CreateColumn(VistaDBType.NChar);
        Convert(this.pattern.Result, column);
        pattern = (string)column.Value;
      }
      finder = new PatternFinder(this.pattern.LineNo, this.pattern.SymbolNo, pattern, escapeCharacter, parent.Connection);
      return true;
    }
  }
}
