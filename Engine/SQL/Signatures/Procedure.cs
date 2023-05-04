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
      parameters = new List<Signature>();
      outParams = new List<bool>();
      namedParams = new Dictionary<string, Signature>(StringComparer.OrdinalIgnoreCase);
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
              parameters.Add(null);
              parser.SkipToken(false);
            }
            else
            {
              Signature signature = parser.NextSignature(false, true, 6);
              namedParams.Add(parameterSignature.Text.Substring(1), signature);
            }
            flag = true;
          }
          else
          {
            if (flag)
              throw new VistaDBSQLException(638, parser.TokenValue.Token, LineNo, SymbolNo);
            parameters.Add(parameterSignature);
          }
          if (parser.IsToken("OUT") || parser.IsToken("OUTPUT"))
          {
            parser.SkipToken(false);
            outParams.Add(true);
          }
          else
            outParams.Add(false);
        }
        else if (parser.IsToken("DEFAULT"))
        {
          if (flag)
            throw new VistaDBSQLException(638, parser.TokenValue.Token, LineNo, SymbolNo);
          parameters.Add(null);
          outParams.Add(false);
          parser.SkipToken(false);
        }
        else
        {
          if (flag)
            throw new VistaDBSQLException(638, parser.TokenValue.Token, LineNo, SymbolNo);
          parameters.Add(parser.NextSignature(false, true, 6));
          outParams.Add(false);
        }
      }
      while (parser.IsToken(",") && parser.SkipToken(true));
    }
  }
}
