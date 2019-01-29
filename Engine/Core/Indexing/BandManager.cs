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
      if (this.isDisposed)
        return;
      this.activeBand = (Band) null;
      this.patternRow = (Row) null;
      this.fileManager = (StorageManager) null;
      for (int index = 0; index < this.Count; ++index)
        this[index].Dispose();
      this.Clear();
      this.isDisposed = true;
      GC.SuppressFinalize((object) this);
    }

    internal Band ActiveBand
    {
      get
      {
        return this.activeBand;
      }
    }

    internal void Flush()
    {
      if (this.Count <= 2)
        return;
      this[this.Count - 3].FlushTailPortion(true);
    }

    internal Band NewActiveBand(int maxKeyCount, int pieces)
    {
      this.activeBand = new Band(this.patternRow, this.fileManager, maxKeyCount, pieces, this.isolatedStorage);
      this.Add(this.activeBand);
      return this.activeBand;
    }

    private void AddBand(Band band)
    {
      this.activeBand = band;
      this.Add(this.activeBand);
    }

    internal void MergeBands()
    {
      if (this.Count < 2)
        return;
      Band band1 = this[this.Count - 1];
      if (band1.Portions > 0)
        band1.FlushTailPortion(false);
      Band band2 = this[this.Count - 2];
      if (band2.Portions > 0)
        band2.FlushTailPortion(false);
      using (BandManager bandManager = new BandManager(this.patternRow, this.fileManager, this.isolatedStorage))
      {
        int pieces = 1;
        do
        {
          pieces *= 2;
          do
          {
            Band band3 = this[this.Count - 1];
            Band band4 = this[this.Count - 2];
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
            this.RemoveAt(this.Count - 1);
            this.RemoveAt(this.Count - 1);
            band5.FlushTailPortion(this.Count > 1);
          }
          while (this.Count > 1);
          for (int index = 0; index < bandManager.Count; ++index)
            this.AddBand(bandManager[index]);
          bandManager.Clear();
        }
        while (this.Count > 1);
      }
    }
  }
}
