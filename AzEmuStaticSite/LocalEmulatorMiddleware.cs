using Dapper;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace AzEmuStaticSite
{
    public class LocalEmulatorMiddleware
    {
        private readonly RequestDelegate _next;
        private readonly IConfiguration cfg;
        private readonly string connectionString = DefaultConnStr;

        private readonly string indexDocument;
        private readonly string notFoundDocument;
        private const string DefaultConnStr = "Server=(localdb)\\MSSQLLocalDB;Database=AzureStorageEmulatorDb510;Trusted_Connection=True;MultipleActiveResultSets=true";

        public LocalEmulatorMiddleware(RequestDelegate next, IConfiguration cfg)
        {
            _next = next;
            this.cfg = cfg;

            this.connectionString = cfg.GetConnectionString("Db") ?? DefaultConnStr;
            this.indexDocument = cfg.GetValue<string>("indexDocument") ?? "index.html";
            this.notFoundDocument = cfg.GetValue<string>("notFoundDocument") ?? "404.html";

            var webContainer = new ContainerInfo()
            {
                AccountName = "devstoreaccount1",
                ContainerName = "$web",
                LastModificationTime = DateTime.UtcNow,
                ServiceMetadata = Encoding.UTF8.GetBytes("SASIdentifiers:\r\n"),
                Metadata = new byte[0]
            };

            var command = @"Insert into BlobContainer ([AccountName]
              ,[ContainerName]
              ,[LastModificationTime]
              ,[ServiceMetadata]
              ,[Metadata]
              ,[LeaseId]
              ,[LeaseState]
              ,[LeaseDuration]
              ,[LeaseEndTime]
              ,[IsLeaseOp])

              values(@AccountName
              , @ContainerName
              , @LastModificationTime
              , @ServiceMetadata
              , @Metadata
              , @LeaseId
              , @LeaseState
              , @LeaseDuration
              , @LeaseEndTime
              , @IsLeaseOp)";

            try
            {
                using var connection = new SqlConnection(this.connectionString);
                connection.Execute(command, webContainer);
            }
            catch (SqlException) {}
        }

        public async Task Invoke(HttpContext httpContext)
        {
            httpContext.Response.Headers.Add("Access-Control-Allow-Methods", "GET");
            httpContext.Response.Headers.Add("Access-Control-Allow-Origin", "*");

            if (httpContext.Request.Method == HttpMethods.Options)
            {
                httpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
                return;
            }

            if(httpContext.Request.Method != HttpMethods.Get)
            {
                httpContext.Response.StatusCode = 500;
                await this.WriteText(httpContext, $"Only GET is supported");
                return;
            }

            if (httpContext.Request.Path.HasValue == false)
            {
                httpContext.Response.StatusCode = 500;
                await this.WriteText(httpContext, $"Root is invalid");
                return;
            }

            var blob = await this.GetBlobInfo(httpContext.Request.Path.Value!.TrimStart('/'));

            if(blob == null)
            {
                blob = await this.GetBlobInfo(httpContext.Request.Path + "/" + this.indexDocument);

                if(blob == null)
                {
                    await this.WriteNotFound(httpContext);
                    return;
                }
            }

            await WriteBlob(httpContext, blob);
        }

        private async Task WriteNotFound(HttpContext httpContext)
        {
            httpContext.Response.StatusCode = 404;

            var blob = await this.GetBlobInfo(this.notFoundDocument);

            if(blob != null)
            {
                await WriteBlob(httpContext, blob);
            }
            else
            {
                await this.WriteText(httpContext, $"404 Not Found - No Default 404 Page was found either");
            }
        }

        private ArrayPool<byte> blockCopyPool = ArrayPool<byte>.Shared;
        private async Task WriteBlob(HttpContext httpContext, BlobInfo blobInfo)
        {
            if(await this.TryCached(httpContext, blobInfo))
            {
                return;
            }

            FileStream currentFile = null;

            await this.WriteBlobHeaders(httpContext, blobInfo);

            try
            {
                var blocks = await this.GetBlocks(blobInfo);

                string? currentFilePath = null;

                foreach(var block in blocks)
                {
                    if(currentFilePath != block.FilePath)
                    {
                        if(currentFile != null)
                        {
                            await currentFile.DisposeAsync();
                        }

                        currentFile = File.Open(block.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read);
                    }

                    if(currentFile == null)
                    {
                        throw new Exception("Error finding backing file for block");
                    }

                    var blockBuffer = blockCopyPool.Rent((int)block.Length);

                    currentFile.Position = block.StartOffset;

                    // Can't use Span overload since we're renting the buffer
                    await currentFile.ReadAsync(blockBuffer, 0, (int)block.Length);

                    await httpContext.Response.Body.WriteAsync(blockBuffer, 0, (int)block.Length);
                }

                await httpContext.Response.Body.FlushAsync();
                return;
            }
            catch (FileNotFoundException)
            {
            }
            catch (DirectoryNotFoundException)
            {
            }
            finally
            {
                currentFile?.Dispose();
            }

            httpContext.Response.StatusCode = 500;
            await this.WriteText(httpContext, $"File backing blocks were not found for '{blobInfo.BlobName}'");
        }

        private async Task<bool> TryCached(HttpContext httpContext, BlobInfo blobInfo)
        {
            if(httpContext.Request.Headers.TryGetValue("If-Modified-Since", out var value)
                && DateTime.TryParseExact(value.ToString(), "ddd, dd MM yyyy HH:mm:ss' GMT'", null, DateTimeStyles.None, out var ifModifiedSince)
                && (blobInfo.LastModificationTime - ifModifiedSince).TotalMilliseconds <= 1000)
            {
                httpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return true;
            }

            return false;
        }

        private async Task WriteBlobHeaders(HttpContext httpContext, BlobInfo blob)
        {
            var headers = httpContext.Response.Headers;

            headers.Add("Content-Type", blob.ContentType);
            headers.Add("Last-Modified", blob.LastModificationTime.ToString("ddd, dd MM yyyy HH:mm:ss 'GMT'"));

            var metadataString = Encoding.UTF8.GetString(blob.ServiceMetadata);

            var properties = metadataString
                .Split("\r\n")
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .Select(l => l.Split(':'))
                .Where(m => m.Length >= 2 && !string.IsNullOrWhiteSpace(m[0]) && !string.IsNullOrWhiteSpace(m[1]))
                .ToDictionary(m => m[0], m => m[1]);

            if(properties.TryGetValue("CacheControl", out var cacheControl))
            {
                headers.Add("Cache-Control", cacheControl);
            }

            if (properties.TryGetValue("ContentDisposition", out var contentDisposition))
            {
                headers.Add("Content-Disposition", contentDisposition);
            }
        }

        private async Task WriteText(HttpContext httpContext, string text)
        {
            using var writer = new StreamWriter(httpContext.Response.Body, leaveOpen: true);
            await writer.WriteLineAsync(text);
            await writer.FlushAsync();
            await httpContext.Response.Body.FlushAsync();
        }

        private async Task<BlobInfo?> GetBlobInfo(string path)
        {
            using var connection = new SqlConnection(this.connectionString);
            var parameters = new { RequestedBlobName = path };
            var blobs = await connection.QueryAsync<BlobInfo>("select top 1 * from Blob where ContainerName = '$web' and BlobName = @RequestedBlobName", parameters);

            return blobs.FirstOrDefault();
        }

        private async Task<IEnumerable<BlockInfo>> GetBlocks(BlobInfo blob)
        {
            using var connection = new SqlConnection(this.connectionString);
            return await connection.QueryAsync<BlockInfo>(@"
                select * from BlockData 
                where AccountName = @AccountName
                    and ContainerName = @ContainerName
                    and BlobName = @BlobName
                    and VersionTimestamp = @VersionTimestamp
                    and IsCommitted = 1", blob);
        }
    }
}
