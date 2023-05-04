using System;
using System.Collections.Generic;
using VistaDB.Engine.Core.IO;

namespace VistaDB.Engine.Core.Indexing
{
  internal class BandManager : List<Band>, IDisposable
  {
    private Band activeBand;
    private Row patternRow;
    private StorageManager fileManager;
    private bool isolatedStorage;
    private bool isDisposed;

    internal BandManager(Row patternRow, StorageManager fileManager, bool isolatedStorage)
    {
      this.patternRow = patternRow;
      this.fileManager = fileManager;
      this.isolatedStorage = isolatedStorage;
    }

    public void Dispose()
    {
      if (isDisposed)
        return;
      activeBand = (Band) null;
      patternRow = (Row) null;
      fileManager = (StorageManager) null;
      for (int index = 0; index < Count; ++index)
        this[index].Dispose();
      Clear();
      isDisposed = true;
      GC.SuppressFinalize((object) this);
    }

    internal Band ActiveBand
    {
      get
      {
        return activeBand;
      }
    }

    internal void Flush()
    {
      if (Count <= 2)
        return;
      this[Count - 3].FlushTailPortion(true);
    }

    internal Band NewActiveBand(int maxKeyCount, int pieces)
    {
      activeBand = new Band(patternRow, fileManager, maxKeyCount, pieces, isolatedStorage);
      Add(activeBand);
      return activeBand;
    }

    private void AddBand(Band band)
    {
      activeBand = band;
      Add(activeBand);
    }

    internal void MergeBands()
    {
      if (Count < 2)
        return;
      Band band1 = this[Count - 1];
      if (band1.Portions > 0)
        band1.FlushTailPortion(false);
      Band band2 = this[Count - 2];
      if (band2.Portions > 0)
        band2.FlushTailPortion(false);
      using (BandManager bandManager = new BandManager(patternRow, fileManager, isolatedStorage))
      {
        int pieces = 1;
        do
        {
          pieces *= 2;
          do
          {
            Band band3 = this[Count - 1];
            Band band4 = this[Count - 2];
            Band band5 = bandManager.NewActiveBand(band3.KeyCount + band4.KeyCount, pieces);
            while (band3.KeyCount > 0 && band4.KeyCount > 0)
            {
              if (band3.PeekKey() - band4.PeekKey() > 0)
                band5.PushKey(band4.PopKey());
              else
                band5.PushKey(band3.PopKey());
            }
            while (band3.KeyCount > 0)
              band5.PushKey(band3.PopKey());
            while (band4.KeyCount > 0)
              band5.PushKey(band4.PopKey());
            RemoveAt(Count - 1);
            RemoveAt(Count - 1);
            band5.FlushTailPortion(Count > 1);
          }
          while (Count > 1);
          for (int index = 0; index < bandManager.Count; ++index)
            AddBand(bandManager[index]);
          bandManager.Clear();
        }
        while (Count > 1);
      }
    }
  }
}
