





//using Microsoft.VisualBasic.Devices;
using System;
using System.Collections;
using System.Collections.Generic;
//using System.Management;
using System.Security;
using VistaDB.Engine.Core.IO;
using System.Linq;
using System.Diagnostics;

namespace VistaDB.Engine.Core.Indexing
{
  internal class SortSpool : List<Row>, IComparer, IDisposable
  {
    private static ulong maxRamAllowed = 1073741823;
    private static SortSpool.MemoryInfo memInfo = new SortSpool.MemoryInfo();
    private int keyCount;
    private BandManager externalBands;
    private Row patternRow;
    private int expectedKeyLength;
    private bool posibleExternalSorting;
    private StorageManager fileManager;
    private bool isolatedStorage;
    private bool isDisposed;

    internal static ulong EstimateMemory()
    {
      try
      {
        return SortSpool.memInfo.AvailableRAM / 4UL;
      }
      catch (SecurityException ex)
      {
        return SortSpool.maxRamAllowed / 4UL;
      }
    }

    internal SortSpool(bool isolatedStorage, uint keyCount, ref int expectedKeyLen, Row patternKey, StorageManager fileManager, bool forceQuickSorting)
    {
      this.fileManager = fileManager;
      this.patternRow = patternKey;
      this.posibleExternalSorting = !forceQuickSorting && patternKey.Extensions == null;
      int num1;
      if (this.posibleExternalSorting)
      {
        this.expectedKeyLength = Band.KeyApartment(patternKey);
        ulong num2 = SortSpool.EstimateMemory() / ((ulong) (this.expectedKeyLength + 100) * 8UL);
        num1 = num2 < (ulong) keyCount ? (int) num2 : (int) keyCount;
      }
      else
        num1 = (int) keyCount;
      this.Capacity = num1 < 0 ? int.MaxValue : num1;
      this.isolatedStorage = isolatedStorage;
      expectedKeyLen = this.expectedKeyLength;
    }

    protected SortSpool()
    {
    }

    internal int KeyCount
    {
      get
      {
        if (this.externalBands != null)
          return this.externalBands.ActiveBand.KeyCount;
        return this.keyCount;
      }
    }

    internal new Row this[int i]
    {
      get
      {
        return base[i];
      }
    }

    private Row PopMemoryKey()
    {
      --this.keyCount;
      Row row = this[keyCount];
  	  this.RemoveAt(keyCount);
      this.Insert(keyCount, (Row) null);
      return row;
    }

    private void MultiPhaseMergingSorting()
    {
      this.OutputSpool();
      this.externalBands.MergeBands();
    }

    private void QuickSorting()
    {
      this.Sort(new Comparison<Row>(this.Compare));
    }

    private void OutputSpool()
    {
      this.QuickSorting();
      try
      {
        if (this.externalBands == null)
          this.externalBands = new BandManager(this.patternRow, this.fileManager, this.isolatedStorage);
        this.externalBands.Flush();
        Band band = this.externalBands.NewActiveBand(this.keyCount, 1);
        while (this.keyCount > 0)
          band.PushKey(this.PopMemoryKey());
      }
      finally
      {
        this.Clear();
      }
    }

    internal void PushKey(Row row, bool forceOutput)
    {
      if (this.posibleExternalSorting && forceOutput)
        this.OutputSpool();
      ++this.keyCount;
      this.Add(row);
    }

    internal Row PopKey()
    {
      if (this.externalBands != null)
        return this.externalBands.ActiveBand.PopKey();
      return this.PopMemoryKey();
    }

    public new void Sort()
    {
      if (this.externalBands == null)
        this.QuickSorting();
      else
        this.MultiPhaseMergingSorting();
    }

    public int Compare(object a, object b)
    {
      return (Row) b - (Row) a;
    }

    public int Compare(Row a, Row b)
    {
      return b - a;
    }

    public void Dispose()
    {
      if (this.isDisposed)
        return;
      GC.SuppressFinalize((object) this);
      if (this.externalBands != null)
        this.externalBands.Dispose();
      this.Clear();
      this.isDisposed = true;
    }

    private class MemoryInfo
    {
      private DateTime lastChecked = DateTime.MinValue;
      private ulong memFree;

      internal MemoryInfo()
      {
      }

      internal ulong AvailableRAM
      {
        get
        {
					using (Process proc = Process.GetCurrentProcess())
					{
						// The proc.PrivateMemorySize64 will returns the private memory usage in byte.
						// Would like to Convert it to Megabyte? divide it by 1e+6
						return Convert.ToUInt64(proc.PrivateMemorySize64 / 1e+6);
					}

					//if (DateTime.Now.Subtract(this.lastChecked).TotalMilliseconds < 15000.0 && this.memFree > 0UL)
					//        return this.memFree;
					//      this.lastChecked = DateTime.Now;
					//      try
					//      {
					//        foreach (ManagementBaseObject managementBaseObject in new ManagementObjectSearcher("SELECT * FROM Win32_OperatingSystem").Get())
					//        {
					//          PropertyData property = managementBaseObject.Properties["FreePhysicalMemory"];
					//          if (property != null)
					//          {
					//            ulong num = (ulong) property.Value * 1024UL;
					//            this.memFree = num > SortSpool.maxRamAllowed ? SortSpool.maxRamAllowed : num;
					//            return this.memFree;
					//          }
					//        }
					//      }
					//      catch
					//      {
					//        try
					//        {
					//          ComputerInfo computerInfo = new ComputerInfo();
					//          this.memFree = computerInfo.AvailablePhysicalMemory > SortSpool.maxRamAllowed ? SortSpool.maxRamAllowed : computerInfo.AvailablePhysicalMemory;
					//          return this.memFree;
					//        }
					//        catch
					//        {
					//          this.memFree = SortSpool.maxRamAllowed;
					//        }
					//      }
					//      return SortSpool.maxRamAllowed;
				}
      }
    }
  }
}
