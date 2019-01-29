﻿using System.Collections.Generic;
using VistaDB.Diagnostic;

namespace VistaDB.Engine.SQL.Signatures
{
  internal abstract class Function : ProgrammabilitySignature
  {
    protected Function(SQLParser parser)
      : base(parser)
    {
    }

    protected Function(SQLParser parser, int paramCount, bool needSkip)
      : base(parser, paramCount, needSkip)
    {
    }

    protected override void ParseParameters(SQLParser parser)
    {
      bool flag;
      if (this.AllowFunctionSyntax() && parser.IsToken("("))
      {
        flag = true;
        parser.SkipToken(true);
      }
      else
      {
        if (!this.AllowProcedureSyntax())
          throw new VistaDBSQLException(500, "\"(\"", this.lineNo, this.symbolNo);
        flag = false;
      }
      this.parameters = new List<Signature>();
      if (!parser.IsToken(")") && (flag || !parser.EndOfText && !parser.IsToken(";")))
      {
        do
        {
          if (parser.IsToken("DEFAULT"))
          {
            this.parameters.Add((Signature) ConstantSignature.CreateSignature(parser));
            parser.SkipToken(false);
          }
          else
            this.parameters.Add(parser.NextSignature(false, true, 6));
        }
        while (parser.IsToken(",") && parser.SkipToken(false));
      }
      if (flag)
        parser.ExpectedExpression(")");
      else
        parser.SuppressNextSkipToken();
    }

    protected virtual bool AllowProcedureSyntax()
    {
      return false;
    }

    protected virtual bool AllowFunctionSyntax()
    {
      return true;
    }
  }
}
