﻿using VistaDB.Engine.Core.Scripting;

namespace VistaDB.Engine.Core
{
  internal class DefaultValue : Filter
  {
    internal DefaultValue(EvalStack evaluation, FilterType typeId, bool autodisp)
      : base(evaluation, typeId, true, autodisp, (int) typeId)
    {
    }
  }
}
