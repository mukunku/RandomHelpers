using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace mukunku.RandomHelpers
{
    /// <summary>
    /// Provides a way to create a read-only seekable s3 stream using the AWS S3 SDK.
    /// To simulate actual seeking new GetObjectRequest's will be issued for each Seek().
    /// Utilize the "TimeWastedSeeking" property to gauge the performance impact for your use-case.
    /// </summary>
    public class SeekableS3Stream : Stream
    {
        private readonly IAmazonS3 s3Client;
        private readonly bool leaveOpen;
        private readonly string bucketName;
        private readonly string keyName;

        private GetObjectResponse latestGetObjectResponse;
        private long fullFileSize;
        private long position = 0;
        private Stopwatch seekingStopWatch = new Stopwatch();

        public override bool CanRead => this.latestGetObjectResponse?.ResponseStream?.CanRead == true;

        public override bool CanSeek => true;

        public override bool CanWrite => false;

        public override long Length => fullFileSize;

        public override long Position { get => this.position; set => this.Seek(value, SeekOrigin.Begin); }

        public TimeSpan TimeWastedSeeking { get => this.seekingStopWatch.Elapsed; }

        public long SeekCount { get; private set; }

        public static Task<Stream> OpenFileAsync(IAmazonS3 s3Client, string bucketName, string keyName, bool leaveClientOpen)
        {
            var seekableStream = new SeekableS3Stream(s3Client, bucketName, keyName, leaveClientOpen);
            try
            {
                return seekableStream.OpenFileStreamAsync();
            }
            catch(Exception)
            {
                seekableStream.Dispose();
                throw;
            }
        }

        public static Stream OpenFile(IAmazonS3 s3Client, string bucketName, string keyName, bool leaveClientOpen)
        {
            var seekableStream = new SeekableS3Stream(s3Client, bucketName, keyName, leaveClientOpen);
            try
            {
                return seekableStream.OpenFileStream();
            }
            catch (Exception)
            {
                seekableStream.Dispose();
                throw;
            }
        }

        private SeekableS3Stream(IAmazonS3 s3Client, string bucketName, string keyName, bool leaveOpen)
        {
            this.s3Client = s3Client;
            this.leaveOpen = leaveOpen;
            this.bucketName = bucketName;
            this.keyName = keyName;
        }

        private async Task<Stream> OpenFileStreamAsync()
        {
            GetObjectRequest request = new GetObjectRequest
            {
                BucketName = this.bucketName,
                Key = this.keyName
            };
            this.latestGetObjectResponse = await this.s3Client.GetObjectAsync(request);
            this.fullFileSize = this.latestGetObjectResponse.ContentLength;
            return this;
        }

        private Stream OpenFileStream()
        {
            GetObjectRequest request = new GetObjectRequest
            {
                BucketName = this.bucketName,
                Key = this.keyName
            };
            this.latestGetObjectResponse = this.s3Client.GetObject(request);
            this.fullFileSize = this.latestGetObjectResponse.ContentLength;
            return this;
        }

        public override void Flush()
        {
            this.latestGetObjectResponse.ResponseStream.Flush();
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            return this.latestGetObjectResponse.ResponseStream.FlushAsync(cancellationToken);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            this.position += count;
            return this.latestGetObjectResponse.ResponseStream.Read(buffer, offset, count);
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            this.position += count;
            return this.latestGetObjectResponse.ResponseStream.ReadAsync(buffer, offset, count, cancellationToken);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            this.SeekCount++;

            long newStreamPos;
            switch(origin)
            {
                case SeekOrigin.Begin:
                    newStreamPos = offset;
                    break;
                case SeekOrigin.End:
                    newStreamPos = this.Length + offset;
                    break;
                case SeekOrigin.Current:
                    newStreamPos = this.position + offset;
                    break;
                default:
                    throw new ArgumentException(nameof(origin));
            }

            if (newStreamPos == this.position)
                return this.position;

            this.latestGetObjectResponse?.Dispose();

            GetObjectRequest request = new GetObjectRequest
            {
                BucketName = this.bucketName,
                Key = this.keyName,
                ByteRange = new ByteRange(newStreamPos, this.Length)
            };

            //No way to do an async Seek unfortunately
            this.seekingStopWatch.Start();
            this.latestGetObjectResponse = this.s3Client.GetObject(request);
            this.seekingStopWatch.Stop();

            this.position = newStreamPos;
            return newStreamPos;
        }       

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                try
                {
                    if (this.latestGetObjectResponse != null)
                        this.latestGetObjectResponse.Dispose();
                }
                catch(Exception) { }

                try
                {
                    if (!this.leaveOpen && this.s3Client != null)
                        this.s3Client.Dispose();
                }
                catch (Exception) { }
            }

            base.Dispose(disposing);
        }
    }
}
