





using System;
using System.IO;
using System.Security.Cryptography;
using System.Security.Permissions;

namespace VistaDB.Engine.Core
{
    [StrongNameIdentityPermission(SecurityAction.LinkDemand, PublicKey = "00240000048000009400000006020000002400005253413100040000010001000fb2ab13e9db180c89e558e0ac32d517f34ddd626fa40293275378577e4a202d2c8095b2327eaac86dc884333d41b1763cfaad61c7bc7e9e959739f08854d71024feff627e8ef86945f430062c4d959bc50da3d27198db758498f406899ab06f1e32fcb6b213525d751e97ec0aa06776bfd21cc9992775a627c317e231d6adc7")]
    public class ActivationKey
    {
        private readonly byte[] m_CurrentKey;
        private readonly byte[] m_Payload;
        private readonly byte[] m_Signature;
        private string m_OwnerCaption;
        private string m_MachineKey;
        private string m_LicenseKey;
        private string m_Version;
        private DateTimeOffset? m_ExpirationDt;
        private DateTimeOffset m_MaintenanceExpirationDt;
        private DateTimeOffset m_GeneratedDt;
        private DateTimeOffset m_RefreshDt;
        private bool m_NewerVersionReleased;

        public ActivationKey()
        {
        }

        public ActivationKey(string activationKey)
          : this(Convert.FromBase64String(activationKey))
        {
        }

        public ActivationKey(byte[] activationKey)
        {
            m_CurrentKey = MachineDecrypt(activationKey);
            using (MemoryStream memoryStream = new MemoryStream(m_CurrentKey))
            {
                int hostValue1;
                BinarySerializer.DeserializeValue(memoryStream, out hostValue1);
                m_Signature = new byte[hostValue1];
                memoryStream.Read(m_Signature, 0, hostValue1);
                long position = memoryStream.Position;
                BinarySerializer.DeserializeValue(memoryStream, out m_OwnerCaption);
                BinarySerializer.DeserializeValue(memoryStream, out m_MachineKey);
                BinarySerializer.DeserializeValue(memoryStream, out m_LicenseKey);
                BinarySerializer.DeserializeValue(memoryStream, out m_Version);
                DateTimeOffset hostValue2;
                BinarySerializer.DeserializeValue(memoryStream, out hostValue2);
                if (hostValue2 > DateTimeOffset.MinValue)
                    ExpirationDt = new DateTimeOffset?(hostValue2);
                BinarySerializer.DeserializeValue(memoryStream, out m_MaintenanceExpirationDt);
                BinarySerializer.DeserializeValue(memoryStream, out m_GeneratedDt);
                BinarySerializer.DeserializeValue(memoryStream, out m_RefreshDt);
                BinarySerializer.DeserializeValue(memoryStream, out m_NewerVersionReleased);
                int count = (int)(m_CurrentKey.LongLength - position);
                m_Payload = new byte[count];
                memoryStream.Position = position;
                memoryStream.Read(m_Payload, 0, count);
            }
        }

        public string MachineKey
        {
            get
            {
                return m_MachineKey;
            }
            set
            {
                m_MachineKey = value;
            }
        }

        public string LicenseKey
        {
            get
            {
                return m_LicenseKey;
            }
            set
            {
                m_LicenseKey = value;
            }
        }

        public DateTimeOffset? ExpirationDt
        {
            get
            {
                return m_ExpirationDt;
            }
            set
            {
                m_ExpirationDt = value;
            }
        }

        public DateTimeOffset MaintenanceExpirationDt
        {
            get
            {
                return m_MaintenanceExpirationDt;
            }
            set
            {
                m_MaintenanceExpirationDt = value;
            }
        }

        public DateTimeOffset GeneratedDt
        {
            get
            {
                return m_GeneratedDt;
            }
            set
            {
                m_GeneratedDt = value;
            }
        }

        public DateTimeOffset RefreshDt
        {
            get
            {
                return m_RefreshDt;
            }
            set
            {
                m_RefreshDt = value;
            }
        }

        public string OwnerCaption
        {
            get
            {
                return m_OwnerCaption;
            }
            set
            {
                m_OwnerCaption = value;
            }
        }

        public string Version
        {
            get
            {
                return m_Version;
            }
            set
            {
                m_Version = value;
            }
        }

        public bool NewerVersionReleased
        {
            get
            {
                return m_NewerVersionReleased;
            }
            set
            {
                m_NewerVersionReleased = value;
            }
        }

        public bool IsValid
        {
            get
            {
                string rsaPublicCspBlob = "BgIAAACkAABSU0ExAAgAAAEAAQAJNvIwsO5Bn9yKzgd5m8e6mAWNZ/xpu6TuhYMv8qmP35Nhp5EhVpd1yCrecy8YDQrq+wRQIP/vVywrRT035ylSjseGl5u8fW5Knya+CBoBBdVGS0Z1bsXN3N/rRhdXkKnkEXuHgJro1lVVMDZt7eZa+FWX19VV1BiIki2bk9gpTqakxdj/Gi9pOMWlNeSrV1jziYYKE3yrGu6if81FHrcLZk8ZzJD0Roc+GhwJpvogWsnhwPM4ivzFTi0JGNP0/MnQxt7Kf9FdxVX7wZ6hx/gMgDP3rL+NzSKZqOuSYGVdnyGP4zfZFP5UYeZaDraglL+S0SpH6WauHprfGCmzSqLh";
                try
                {
                    return VerifySignature(rsaPublicCspBlob);
                }
                catch (Exception)
                {
                    return false;
                }
            }
        }

        public bool VerifySignature(string rsaPublicCspBlob)
        {
            if (m_CurrentKey == null || m_Payload == null || (m_Signature == null || m_CurrentKey.Length == 0) || (m_Payload.Length == 0 || m_Signature.Length == 0))
                return false;
            byte[] keyBlob = Convert.FromBase64String(rsaPublicCspBlob);
            using (RSACryptoServiceProvider cryptoServiceProvider1 = new RSACryptoServiceProvider(new CspParameters()
            {
                Flags = CspProviderFlags.UseMachineKeyStore | CspProviderFlags.NoPrompt
            }))
            {
                VistaDBLicensing.FixOwnerPermissions(cryptoServiceProvider1.CspKeyContainerInfo, false, false);
                cryptoServiceProvider1.PersistKeyInCsp = false;
                cryptoServiceProvider1.ImportCspBlob(keyBlob);
                try
                {
                    using (SHA1CryptoServiceProvider cryptoServiceProvider2 = new SHA1CryptoServiceProvider())
                    {
                        byte[] hash = cryptoServiceProvider2.ComputeHash(m_Payload);
                        return cryptoServiceProvider1.VerifyHash(hash, "SHA1", m_Signature);
                    }
                }
                catch
                {
                    return false;
                }
            }
        }

        public static bool ValidPublicKeyCspBlob(string rsaPublicCspBlob)
        {
            if (string.IsNullOrEmpty(rsaPublicCspBlob))
                return false;
            try
            {
                byte[] keyBlob = Convert.FromBase64String(rsaPublicCspBlob);
                using (RSACryptoServiceProvider cryptoServiceProvider = new RSACryptoServiceProvider(new CspParameters()
                {
                    Flags = CspProviderFlags.UseMachineKeyStore | CspProviderFlags.NoPrompt
                }))
                {
                    VistaDBLicensing.FixOwnerPermissions(cryptoServiceProvider.CspKeyContainerInfo, false, false);
                    cryptoServiceProvider.PersistKeyInCsp = false;
                    cryptoServiceProvider.ImportCspBlob(keyBlob);
                    return cryptoServiceProvider.PublicOnly;
                }
            }
            catch
            {
                return false;
            }
        }

        private static byte[] MachineDecrypt(byte[] activationKey)
        {
            int count;
            byte[] buffer;
            byte[] numArray1;
            using (RSACryptoServiceProvider localKey = VistaDBLicensing.GetLocalKey())
            {
                localKey.PersistKeyInCsp = true;
                byte[] numArray2;
                using (MemoryStream memoryStream = new MemoryStream(activationKey))
                {
                    int hostValue;
                    BinarySerializer.DeserializeValue(memoryStream, out hostValue);
                    numArray2 = new byte[hostValue];
                    memoryStream.Read(numArray2, 0, hostValue);
                    count = activationKey.Length - (int)memoryStream.Position;
                    buffer = new byte[count];
                    memoryStream.Read(buffer, 0, count);
                }
                numArray1 = localKey.Decrypt(numArray2, false);
            }
            byte[] numArray3 = new byte[16];
            int length = numArray1.Length - 16;
            byte[] numArray4 = new byte[length];
            int num = 0;
            for (int index = 0; index < 16; ++index)
                numArray3[index] = numArray1[num++];
            for (int index = 0; index < length; ++index)
                numArray4[index] = numArray1[num++];
            using (MemoryStream memoryStream = new MemoryStream())
            {
                using (RijndaelManaged rijndaelManaged = new RijndaelManaged())
                {
                    rijndaelManaged.BlockSize = 128;
                    rijndaelManaged.Key = numArray4;
                    rijndaelManaged.IV = numArray3;
                    rijndaelManaged.Mode = CipherMode.CFB;
                    rijndaelManaged.Padding = PaddingMode.None;
                    rijndaelManaged.FeedbackSize = 8;
                    using (ICryptoTransform decryptor = rijndaelManaged.CreateDecryptor())
                    {
                        using (CryptoStream cryptoStream = new CryptoStream(memoryStream, decryptor, CryptoStreamMode.Write))
                        {
                            cryptoStream.Write(buffer, 0, count);
                            cryptoStream.Close();
                        }
                    }
                }
                return memoryStream.ToArray();
            }
        }
    }
}
