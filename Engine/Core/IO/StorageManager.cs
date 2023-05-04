using System;
using System.Collections.Generic;
using System.IO;
using System.Security;

namespace VistaDB.Engine.Core.IO
{
    internal class StorageManager : List<StorageHandle>, IDisposable
    {
        private static readonly Random random = new Random();
        private static object SyncObj = new object();
        private bool serverMode;
        private bool isDisposed;

        internal StorageManager()
        {
        }

        internal bool ServerMode
        {
            get
            {
                return false;
            }
        }

        private StorageHandle LookForSameCompatible(string fileName, StorageHandle.StorageMode mode)
        {
            for (int index = 0; index <= Count - 1; ++index)
            {
                StorageHandle storageHandle = this[index];
                if (!(fileName != storageHandle.Name))
                {
                    storageHandle.CheckCompatibility(mode, true);
                    return storageHandle;
                }
            }
            return null;
        }

        internal StorageHandle CreateTemporaryStorage(int SizeofPage, bool transacted, bool isolated)
        {
            string str;
            try
            {
                str = Path.GetTempFileName();
                File.Delete(str);
            }
            catch (SecurityException)
            {
                str = "VistaDB." + Guid.NewGuid().ToString() + ".tmp";
            }
            return CreateTemporaryStorage(str, SizeofPage, transacted, isolated);
        }

        internal StorageHandle CreateTemporaryStorage(string fileName, int SizeofPage, bool transacted, bool isolated)
        {
            return OpenStorage(fileName, new StorageHandle.StorageMode(FileMode.CreateNew, FileShare.None, FileAccess.ReadWrite, FileAttributes.Hidden | FileAttributes.Archive | FileAttributes.Temporary | FileAttributes.NotContentIndexed, transacted, true, isolated), SizeofPage, false);
        }

        internal StorageHandle OpenOrCreateTemporaryStorage(string fileName, bool shared, int SizeofPage, bool isolated, bool persistent)
        {
            return OpenStorage(fileName, new StorageHandle.StorageMode(FileMode.OpenOrCreate, shared ? FileShare.ReadWrite : FileShare.None, FileAccess.ReadWrite, FileAttributes.Hidden | FileAttributes.Archive | FileAttributes.Temporary, false, !persistent, isolated), SizeofPage, persistent);
        }

        internal StorageHandle OpenStorage(string fileName, StorageHandle.StorageMode mode, int SizeofPage, bool persistent)
        {
            lock (SyncObj)
            {
                if (serverMode && (mode.Attributes & FileAttributes.Temporary) != FileAttributes.Temporary)
                {
                    StorageHandle storageHandle = LookForSameCompatible(fileName, mode);
                    if (storageHandle != null)
                    {
                        storageHandle.AddRef();
                        return storageHandle;
                    }
                    mode.SetExclusive();
                }
                mode.VirtualLocks = mode.VirtualLocks || ServerMode;
                StorageHandle storageHandle1 = new StorageHandle(fileName, mode, SizeofPage, persistent);
                Add(storageHandle1);
                storageHandle1.AddRef();
                return storageHandle1;
            }
        }

        internal void CloseStorage(StorageHandle handle)
        {
            if (handle == null)
                return;
            lock (SyncObj)
            {
                if (!Contains(handle) || !handle.ReleaseRef())
                    return;
                Remove(handle);
                handle.Dispose();
            }
        }

        public void Dispose()
        {
            lock (SyncObj)
            {
                if (!isDisposed)
                {
                    Destroy();
                    isDisposed = true;
                }
                GC.SuppressFinalize(this);
            }
        }

        ~StorageManager()
        {
            if (isDisposed)
                return;
            Destroy();
            isDisposed = true;
        }

        private void Destroy()
        {
            try
            {
                foreach (StorageHandle storageHandle in (List<StorageHandle>)this)
                    storageHandle?.Dispose();
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                Clear();
            }
        }
    }
}
