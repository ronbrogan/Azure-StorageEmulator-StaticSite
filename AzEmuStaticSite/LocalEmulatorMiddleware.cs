using Dapper;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using System;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzEmuStaticSite
{
    public class LocalEmulatorMiddleware
    {
        private readonly RequestDelegate _next;
        private readonly IConfiguration cfg;
        private readonly SqlConnection connection;

        private readonly string indexDocument;
        private readonly string notFoundDocument;
        private const string DefaultConnStr = "Server=(localdb)\\MSSQLLocalDB;Database=AzureStorageEmulatorDb510;Trusted_Connection=True;MultipleActiveResultSets=true";

        public LocalEmulatorMiddleware(RequestDelegate next, IConfiguration cfg)
        {
            _next = next;
            this.cfg = cfg;

            var connStr = cfg.GetConnectionString("Db") ?? DefaultConnStr;
            this.connection = new SqlConnection(connStr);
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
                this.connection.Execute(command, webContainer);
            }
            catch (SqlException) {}
        }

        public async Task Invoke(HttpContext httpContext)
        {
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

            await WriteDocument(httpContext, Path.Combine(blob.DirectoryPath, blob.FileName ?? "1"));
        }

        private async Task WriteNotFound(HttpContext httpContext)
        {
            httpContext.Response.StatusCode = 404;

            var blob = await this.GetBlobInfo(this.notFoundDocument);

            if(blob != null)
            {
                await WriteDocument(httpContext, Path.Combine(blob.FileName, "1"));
            }
            else
            {
                await this.WriteText(httpContext, $"404 Not Found - No Default 404 Page was found either");
            }
        }

        private async Task WriteDocument(HttpContext httpContext, string filePath)
        {
            try
            {
                using var f = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
                await f.CopyToAsync(httpContext.Response.Body);
                await httpContext.Response.Body.FlushAsync();
                return;
            }
            catch (FileNotFoundException)
            {
            }
            catch (DirectoryNotFoundException)
            {
            }

            httpContext.Response.StatusCode = 500;
            await this.WriteText(httpContext, $"File backing blob was not found at '{filePath}'");
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
            var parameters = new { RequestedBlobName = path };
            var blobs = await this.connection.QueryAsync<BlobInfo>("select top 1 * from Blob where ContainerName = '$web' and BlobName = @RequestedBlobName", parameters);

            return blobs.FirstOrDefault();
        }
    }
}
