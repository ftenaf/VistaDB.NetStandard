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
      this.signatureType = SignatureType.Expression;
      this.dataType = VistaDBType.Bit;
      this.expression = expression;
      this.finder = (PatternFinder) null;
      this.exprResult = (IColumn) null;
      this.pattern = parser.NextSignature(true, true, 2);
      if (parser.IsToken("ESCAPE"))
      {
        parser.SkipToken(true);
        if (parser.TokenValue.TokenType != TokenType.String)
          throw new VistaDBSQLException(553, "", this.lineNo, this.symbolNo);
        this.escapeCharacter = parser.TokenValue.Token;
        if (this.escapeCharacter.Length != 1)
          throw new VistaDBSQLException(553, "", this.lineNo, this.symbolNo);
        parser.SkipToken(false);
      }
      else
        this.escapeCharacter = (string) null;
    }

    public override SignatureType OnPrepare()
    {
      this.expression = ConstantSignature.PrepareAndCheckConstant(this.expression, VistaDBType.NChar);
      if (!Utils.CompatibleTypes(this.expression.DataType, VistaDBType.NChar))
        throw new VistaDBSQLException(559, "", this.lineNo, this.symbolNo);
      if (!Utils.IsCharacterDataType(this.expression.DataType))
        this.exprResult = this.CreateColumn(VistaDBType.NChar);
      this.pattern = ConstantSignature.PrepareAndCheckConstant(this.pattern, VistaDBType.NChar);
      if (!Utils.CompatibleTypes(this.pattern.DataType, VistaDBType.NChar))
        throw new VistaDBSQLException(552, "", this.pattern.LineNo, this.pattern.SymbolNo);
      this.CalcOptimizeLevel();
      if (this.expression.SignatureType == SignatureType.Constant && this.pattern.SignatureType == SignatureType.Constant || (this.expression.AlwaysNull || this.pattern.AlwaysNull))
        return SignatureType.Constant;
      return this.signatureType;
    }

    protected override bool OnOptimize(ConstraintOperations constrainOperations)
    {
      int chunkCount;
      if (!this.CreatePattern() || this.finder.GetOptimizationLevel(out chunkCount) != OptimizationLevel.Full)
        return false;
      Signature low;
      Signature high;
      this.finder.GetOptimizationScopeSignatures(this.parent, out low, out high);
      if (chunkCount > 1)
        return constrainOperations.AddLogicalBetween((ColumnSignature) this.expression, low, high, false);
      return constrainOperations.AddLogicalCompare(this.expression, low, CompareOperation.Equal, CompareOperation.Equal, false);
    }

    protected override IColumn InternalExecute()
    {
      if (!this.AlwaysNull && this.GetIsChanged())
      {
        IColumn column = this.expression.Execute();
        if (!this.CreatePattern())
        {
          ((IValue) this.result).Value = (object) false;
        }
        else
        {
          string matchExpr;
          if (this.exprResult != null)
          {
            this.Convert((IValue) column, (IValue) this.exprResult);
            matchExpr = (string) ((IValue) this.exprResult).Value;
          }
          else
            matchExpr = (string) ((IValue) column).Value;
			result.Value = column.IsNull ? false : (Compare(matchExpr) ? true : false);
        }
      }
      return this.result;
    }

    protected virtual bool Compare(string matchExpr)
    {
      return this.finder.CompareWithRegEx(matchExpr) > 0;
    }

    protected override bool IsEquals(Signature signature)
    {
      if (signature.GetType() != this.GetType())
        return false;
      LikeOperator likeOperator = (LikeOperator) signature;
      if (this.expression == likeOperator.expression && this.pattern == likeOperator.pattern)
        return this.parent.Connection.CompareString(this.escapeCharacter, likeOperator.escapeCharacter, true) == 0;
      return false;
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
      this.expression = this.expression.Relink(signature, ref columnCount);
      this.pattern = this.expression.Relink(signature, ref columnCount);
    }

    public override void SetChanged()
    {
      this.expression.SetChanged();
      this.pattern.SetChanged();
    }

    public override void ClearChanged()
    {
      this.expression.ClearChanged();
      this.pattern.ClearChanged();
    }

    protected override bool InternalGetIsChanged()
    {
      if (!this.expression.GetIsChanged())
        return this.pattern.GetIsChanged();
      return true;
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
      this.expression.GetAggregateFunctions(list);
      this.pattern.GetAggregateFunctions(list);
    }

    public override int ColumnCount
    {
      get
      {
        return this.expression.ColumnCount + this.pattern.ColumnCount;
      }
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      bool distinct1;
      bool distinct2;
      bool flag = this.expression.HasAggregateFunction(out distinct1) | this.expression.HasAggregateFunction(out distinct2);
      distinct = distinct1 || distinct2;
      return flag;
    }

    public override bool AlwaysNull
    {
      get
      {
        if (!this.expression.AlwaysNull)
          return this.pattern.AlwaysNull;
        return true;
      }
    }

    public override bool IsNull
    {
      get
      {
        if (!this.expression.IsNull)
          return this.pattern.IsNull;
        return true;
      }
    }

    private void CalcOptimizeLevel()
    {
      if (this.expression.SignatureType != SignatureType.Column)
      {
        this.optimizable = false;
      }
      else
      {
        switch (this.pattern.SignatureType)
        {
          case SignatureType.Constant:
          case SignatureType.Parameter:
          case SignatureType.ExternalColumn:
            this.optimizable = true;
            break;
          default:
            this.optimizable = false;
            break;
        }
      }
    }

    protected bool CreatePattern()
    {
      if (!this.pattern.GetIsChanged())
        return this.finder != null;
      this.pattern.Execute();
      if (this.pattern.Result.IsNull)
        return false;
      string pattern;
      if (Utils.IsCharacterDataType(this.pattern.DataType))
      {
        pattern = (string) ((IValue) this.pattern.Execute()).Value;
      }
      else
      {
        IColumn column = this.CreateColumn(VistaDBType.NChar);
        this.Convert((IValue) this.pattern.Result, (IValue) column);
        pattern = (string) ((IValue) column).Value;
      }
      this.finder = new PatternFinder(this.pattern.LineNo, this.pattern.SymbolNo, pattern, this.escapeCharacter, this.parent.Connection);
      return true;
    }
  }
}
