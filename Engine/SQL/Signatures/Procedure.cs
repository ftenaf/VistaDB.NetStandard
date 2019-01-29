using System;
using System.Collections.Generic;
using VistaDB.Diagnostic;

namespace VistaDB.Engine.SQL.Signatures
{
  internal abstract class Procedure : ProgrammabilitySignature
  {
    protected List<SQLParser.VariableDeclaration> variables;
    protected Dictionary<string, Signature> namedParams;
    protected List<bool> outParams;

    protected Procedure(SQLParser parser, int paramCount, bool needSkip)
      : base(parser, paramCount, needSkip)
    {
    }

    protected override void ParseParameters(SQLParser parser)
    {
      this.parameters = new List<Signature>();
      this.outParams = new List<bool>();
      this.namedParams = new Dictionary<string, Signature>((IEqualityComparer<string>) StringComparer.OrdinalIgnoreCase);
      bool flag = false;
      if (parser.IsToken(";") || parser.EndOfText)
        return;
      do
      {
        if (ParameterSignature.IsParameter(parser.TokenValue.Token))
        {
          ParameterSignature parameterSignature = parser.NextSignature(false, true, -1) as ParameterSignature;
          if (parser.IsToken("="))
          {
            parser.SkipToken(true);
            if (parser.IsToken("DEFAULT"))
            {
              this.parameters.Add((Signature) null);
              parser.SkipToken(false);
            }
            else
            {
              Signature signature = parser.NextSignature(false, true, 6);
              this.namedParams.Add(parameterSignature.Text.Substring(1), signature);
            }
            flag = true;
          }
          else
          {
            if (flag)
              throw new VistaDBSQLException(638, parser.TokenValue.Token, this.LineNo, this.SymbolNo);
            this.parameters.Add((Signature) parameterSignature);
          }
          if (parser.IsToken("OUT") || parser.IsToken("OUTPUT"))
          {
            parser.SkipToken(false);
            this.outParams.Add(true);
          }
          else
            this.outParams.Add(false);
        }
        else if (parser.IsToken("DEFAULT"))
        {
          if (flag)
            throw new VistaDBSQLException(638, parser.TokenValue.Token, this.LineNo, this.SymbolNo);
          this.parameters.Add((Signature) null);
          this.outParams.Add(false);
          parser.SkipToken(false);
        }
        else
        {
          if (flag)
            throw new VistaDBSQLException(638, parser.TokenValue.Token, this.LineNo, this.SymbolNo);
          this.parameters.Add(parser.NextSignature(false, true, 6));
          this.outParams.Add(false);
        }
      }
      while (parser.IsToken(",") && parser.SkipToken(true));
    }
  }
}
