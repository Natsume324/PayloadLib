using ChromeosUpdateEngine;
using Google.Protobuf;
using ICSharpCode.SharpZipLib.BZip2;
using SharpCompress.Compressors.Xz;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace PayloadLib
{
    public class PartitionInfo
    {
        public int Index { get; set; }
        public string Name { get; set; }
        public long Size { get; set; }
        public int OperationCount { get; set; }
    }

    public class PayloadParser : IDisposable
    {
        private const string Magic = "CrAU";
        private readonly string _payloadPath;
        private DeltaArchiveManifest _manifest;
        private long _dataStart;
        private List<PartitionInfo> _partitions;
        private FileStream _sharedFileStream;
        private readonly object _fileAccessLock = new object();

        public PayloadParser(string payloadPath)
        {
            if (!File.Exists(payloadPath))
                throw new FileNotFoundException("Payload file not found", payloadPath);

            _payloadPath = payloadPath;
            ParseManifest();
        }

        private void ParseManifest()
        {
            using var reader = new BinaryReader(File.OpenRead(_payloadPath));

            if (Encoding.ASCII.GetString(reader.ReadBytes(4)) != Magic)
                throw new InvalidDataException("Invalid payload magic header");

            ulong version = BitConverter.ToUInt64(reader.ReadBytes(8).Reverse().ToArray(), 0);
            if (version < 2)
                throw new NotSupportedException("Only payload version 2+ is supported");

 
            ulong manifestSize = BitConverter.ToUInt64(reader.ReadBytes(8).Reverse().ToArray(), 0);
            uint sigSize = BitConverter.ToUInt32(reader.ReadBytes(4).Reverse().ToArray(), 0);

            _manifest = new MessageParser<DeltaArchiveManifest>(() => new DeltaArchiveManifest())
                .ParseFrom(reader.ReadBytes((int)manifestSize));

            reader.BaseStream.Seek(sigSize, SeekOrigin.Current);
            _dataStart = reader.BaseStream.Position;

            _partitions = new List<PartitionInfo>();
            for (int i = 0; i < _manifest.Partitions.Count; i++)
            {
                var partition = _manifest.Partitions[i];
                _partitions.Add(new PartitionInfo
                {
                    Index = i,
                    Name = partition.PartitionName,
                    Size = Convert.ToInt64(partition.NewPartitionInfo?.Size),
                    OperationCount = partition.Operations.Count
                });
            }
        }

        public IReadOnlyList<PartitionInfo> GetPartitions()
        {
            return _partitions.AsReadOnly();
        }

        public void ExtractPartition(int partitionIndex, string outputPath)
        {
            using var stream = GetPartitionStream(partitionIndex);
            using var outputStream = File.Create(outputPath);
            stream.CopyTo(outputStream);
        }

        public PartitionStream GetPartitionStream(int partitionIndex)
        {
            if (partitionIndex < 0 || partitionIndex >= _partitions.Count)
                throw new ArgumentOutOfRangeException(nameof(partitionIndex), "Invalid partition index");

            var partition = _manifest.Partitions[partitionIndex];
            return new PartitionStream(
                _payloadPath,
                _dataStart,
                _manifest,
                partition,
                _fileAccessLock);
        }

        public void Dispose()
        {
            _sharedFileStream?.Dispose();
            GC.SuppressFinalize(this);
        }
    }

    public class PartitionStream : Stream
    {
        private readonly string _filePath;
        private readonly long _dataStart;
        private readonly DeltaArchiveManifest _manifest;
        private readonly PartitionUpdate _partition;
        private readonly object _fileAccessLock;

        private int _currentOperationIndex = 0;
        private Stream _currentOperationStream;
        private long _position;
        private readonly long _length;
        private FileStream _fileStream;
        private readonly byte[] _zeroBuffer = new byte[1024 * 1024]; 

        public int CurrentOperationIndex => _currentOperationIndex;
        public int TotalOperations => _partition.Operations.Count;
        public string PartitionName => _partition.PartitionName;

        internal PartitionStream(
            string filePath,
            long dataStart,
            DeltaArchiveManifest manifest,
            PartitionUpdate partition,
            object fileAccessLock)
        {
            _filePath = filePath;
            _dataStart = dataStart;
            _manifest = manifest;
            _partition = partition;
            _fileAccessLock = fileAccessLock;
            _length = (long)partition.NewPartitionInfo.Size;
            InitializeNextOperation();
        }

        private void InitializeNextOperation()
        {
            if (_currentOperationIndex >= _partition.Operations.Count)
                return;

            var operation = _partition.Operations[_currentOperationIndex];
            long dstLength = (long)operation.DstExtents[0].NumBlocks * _manifest.BlockSize;

            switch (operation.Type)
            {
                case InstallOperation.Types.Type.Replace:
                    _currentOperationStream = new ReplaceOperationStream(
                        _filePath,
                        _dataStart + (long)operation.DataOffset,
                        (long)operation.DataLength,
                        _fileAccessLock);
                    break;

                case InstallOperation.Types.Type.ReplaceBz:
                    _currentOperationStream = new BZip2OperationStream(
                        _filePath,
                        _dataStart + (long)operation.DataOffset,
                        (long)operation.DataLength,
                        _fileAccessLock);
                    break;

                case InstallOperation.Types.Type.ReplaceXz:
                    _currentOperationStream = new XzOperationStream(
                        _filePath,
                        _dataStart + (long)operation.DataOffset,
                        (long)operation.DataLength,
                        _fileAccessLock);
                    break;

                case InstallOperation.Types.Type.Zero:
                    _currentOperationStream = new ZeroOperationStream(dstLength);
                    break;

                default:
                    throw new NotSupportedException($"Operation type {operation.Type} is not supported.");
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_position >= _length)
                return 0;

            int totalBytesRead = 0;
            while (count > 0 && _currentOperationIndex < _partition.Operations.Count)
            {
                int bytesRead = _currentOperationStream.Read(buffer, offset, count);

                if (bytesRead == 0)
                {
                    _currentOperationIndex++;
                    if (_currentOperationIndex < _partition.Operations.Count)
                    {
                        _currentOperationStream?.Dispose();
                        InitializeNextOperation();
                    }
                    continue;
                }

                totalBytesRead += bytesRead;
                offset += bytesRead;
                count -= bytesRead;
                _position += bytesRead;
            }

            return totalBytesRead;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _currentOperationStream?.Dispose();
                _fileStream?.Dispose();
            }
            base.Dispose(disposing);
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => _length;
        public override long Position
        {
            get => _position;
            set => throw new NotSupportedException();
        }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    #region Operation Stream Implementations

    internal abstract class OperationStream : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    internal class ReplaceOperationStream : OperationStream
    {
        private readonly string _filePath;
        private readonly long _filePosition;
        private readonly long _length;
        private readonly object _fileAccessLock;
        private FileStream _fileStream;
        private long _position;

        public ReplaceOperationStream(
            string filePath,
            long filePosition,
            long length,
            object fileAccessLock)
        {
            _filePath = filePath;
            _filePosition = filePosition;
            _length = length;
            _fileAccessLock = fileAccessLock;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            lock (_fileAccessLock)
            {
                if (_fileStream == null)
                {
                    _fileStream = File.OpenRead(_filePath);
                    _fileStream.Seek(_filePosition, SeekOrigin.Begin);
                }

                long remaining = _length - _position;
                if (remaining <= 0) return 0;

                int bytesToRead = (int)Math.Min(count, remaining);
                int bytesRead = _fileStream.Read(buffer, offset, bytesToRead);
                _position += bytesRead;
                return bytesRead;
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _fileStream?.Dispose();
            }
            base.Dispose(disposing);
        }
    }

    internal class BZip2OperationStream : OperationStream
    {
        private readonly string _filePath;
        private readonly long _filePosition;
        private readonly long _compressedLength;
        private readonly object _fileAccessLock;
        private BZip2InputStream _decompressionStream;
        private bool _initialized;

        public BZip2OperationStream(
            string filePath,
            long filePosition,
            long compressedLength,
            object fileAccessLock)
        {
            _filePath = filePath;
            _filePosition = filePosition;
            _compressedLength = compressedLength;
            _fileAccessLock = fileAccessLock;
        }

        private void Initialize()
        {
            if (_initialized) return;

            lock (_fileAccessLock)
            {
                using var fileStream = File.OpenRead(_filePath);
                fileStream.Seek(_filePosition, SeekOrigin.Begin);

                byte[] compressedData = new byte[_compressedLength];
                fileStream.Read(compressedData, 0, compressedData.Length);

                var compressedStream = new MemoryStream(compressedData);
                _decompressionStream = new BZip2InputStream(compressedStream);
            }
            _initialized = true;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (!_initialized) Initialize();
            return _decompressionStream.Read(buffer, offset, count);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _decompressionStream?.Dispose();
            }
            base.Dispose(disposing);
        }
    }

    internal class XzOperationStream : OperationStream
    {
        private readonly string _filePath;
        private readonly long _filePosition;
        private readonly long _compressedLength;
        private readonly object _fileAccessLock;
        private XZStream _decompressionStream;
        private bool _initialized;

        public XzOperationStream(
            string filePath,
            long filePosition,
            long compressedLength,
            object fileAccessLock)
        {
            _filePath = filePath;
            _filePosition = filePosition;
            _compressedLength = compressedLength;
            _fileAccessLock = fileAccessLock;
        }

        private void Initialize()
        {
            if (_initialized) return;

            lock (_fileAccessLock)
            {
                using var fileStream = File.OpenRead(_filePath);
                fileStream.Seek(_filePosition, SeekOrigin.Begin);

                byte[] compressedData = new byte[_compressedLength];
                fileStream.Read(compressedData, 0, compressedData.Length);

                var compressedStream = new MemoryStream(compressedData);
                _decompressionStream = new XZStream(compressedStream);
            }
            _initialized = true;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (!_initialized) Initialize();
            return _decompressionStream.Read(buffer, offset, count);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _decompressionStream?.Dispose();
            }
            base.Dispose(disposing);
        }
    }

    internal class ZeroOperationStream : OperationStream
    {
        private readonly long _length;
        private long _position;
        private readonly byte[] _zeroBuffer = new byte[1024 * 1024]; 

        public ZeroOperationStream(long length)
        {
            _length = length;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            long remaining = _length - _position;
            if (remaining <= 0) return 0;

            int bytesToRead = (int)Math.Min(count, remaining);
            Array.Clear(buffer, offset, bytesToRead);
            _position += bytesToRead;
            return bytesToRead;
        }
    }

    #endregion
}