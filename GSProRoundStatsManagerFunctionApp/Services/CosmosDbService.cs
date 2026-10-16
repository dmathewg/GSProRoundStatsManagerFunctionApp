using Azure.Core;
using Azure.Identity;
using GSProRoundStatsManagerFunctionApp.ConfigurationOptions;
using GSProRoundStatsManagerFunctionApp.Models.Stats;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Net;

namespace GSProRoundStatsManagerFunctionApp.Services
{
    // Trimmed to the three operations RoundStatsManager actually calls. The point-read,
    // create/upsert/patch, single-delete and LINQ query methods live in the Portal's
    // CosmosDbService if they're ever needed here.
    public interface ICosmosDbService<T> where T : class
    {
        Task<IEnumerable<T>> ReadFeedAsync(string partitionKey);
        Task<IEnumerable<T>> QueryItemsAsync(QueryDefinition query, string? partitionKey);
        Task<bool> DeleteItemsInParallelAsync(List<RoundStat> itemsToDelete);
    }

    public class CosmosDbService<T> : ICosmosDbService<T> where T : class
    {
        private readonly Container _container;
        private readonly ILogger<CosmosDbService<T>> _logger;

        // Caps in-flight write concurrency. Without this, DeleteItemsInParallelAsync fired
        // 100 concurrent deletes, generated 429s, and the SDK retried them internally with
        // backoff - invisible in App Insights, visible only as wall-clock time.
        private readonly SemaphoreSlim _writeLimiter = new SemaphoreSlim(16, 16);

        public CosmosDbService(CosmosClient cosmosClient, string databaseName, string containerName, ILogger<CosmosDbService<T>> logger)
        {
            _container = cosmosClient.GetContainer(databaseName, containerName);
            _logger = logger;
        }

        // ReadFeed items
        public async Task<IEnumerable<T>> ReadFeedAsync(string partitionKey)
        {
            var sw = Stopwatch.StartNew();

            try
            {
                using FeedIterator<T> iterator = _container.GetItemQueryIterator<T>(
                    requestOptions: new QueryRequestOptions
                    {
                        PartitionKey = new PartitionKey(partitionKey),
                        MaxItemCount = -1
                    });

                List<T> results = new List<T>();
                double totalRu = 0;
                int pages = 0;

                while (iterator.HasMoreResults)
                {
                    var response = await iterator.ReadNextAsync();
                    totalRu += response.RequestCharge;
                    pages++;
                    results.AddRange(response);
                }

                sw.Stop();
                LogOp("Read", partitionKey, sw.ElapsedMilliseconds, totalRu, results.Count, pages);

                return results;
            }
            catch (CosmosException ex)
            {
                LogCosmosException("Read", ex, partitionKey, null);
                throw;
            }
        }

        // Query items
        public async Task<IEnumerable<T>> QueryItemsAsync(QueryDefinition queryDefinition, string? partitionKey)
        {
            var sw = Stopwatch.StartNew();

            try
            {
                using FeedIterator<T> queryIterator = _container.GetItemQueryIterator<T>(queryDefinition, null,
                    partitionKey == null ? new QueryRequestOptions()
                    {
                        MaxItemCount = -1
                    } : new QueryRequestOptions()
                    {
                        PartitionKey = new PartitionKey(partitionKey),
                        MaxItemCount = -1
                    });

                List<T> results = new List<T>();
                double totalRu = 0;
                int pages = 0;

                while (queryIterator.HasMoreResults)
                {
                    var response = await queryIterator.ReadNextAsync();
                    totalRu += response.RequestCharge;
                    pages++;
                    results.AddRange(response);
                }

                sw.Stop();
                LogOp("Query", partitionKey, sw.ElapsedMilliseconds, totalRu, results.Count, pages);

                return results;
            }
            catch (CosmosException ex)
            {
                LogCosmosException("Query", ex, partitionKey, null);
                throw;
            }
        }

        // Delete many items, throttle-aware
        public async Task<bool> DeleteItemsInParallelAsync(List<RoundStat> itemsToDelete)
        {
            // Batching into groups of 100 was replaced by the semaphore: batching still let
            // 100 requests go out at once and made each batch wait for its slowest member.
            // The limiter keeps a steady 16 in flight with no barrier between them.
            var sw = Stopwatch.StartNew();
            int deleted = 0;
            int notFound = 0;
            int throttled = 0;
            int failed = 0;
            double ruCharge = 0;
            object lockObject = new object();

            var tasks = itemsToDelete.Select(async item =>
            {
                await _writeLimiter.WaitAsync();
                try
                {
                    var response = await _container.DeleteItemAsync<T>(item.id, new PartitionKey(item.RoundFileHash));
                    Interlocked.Increment(ref deleted);
                    lock (lockObject)
                    {
                        ruCharge += response.RequestCharge;
                    }
                }
                catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    // Already gone - not an error for a cleanup job.
                    Interlocked.Increment(ref notFound);
                }
                catch (CosmosException ex)
                {
                    // Swallow per-item failures so one bad doc doesn't abort the whole run.
                    if (ex.StatusCode == HttpStatusCode.TooManyRequests)
                    {
                        Interlocked.Increment(ref throttled);
                    }

                    Interlocked.Increment(ref failed);

                    _logger.LogError(
                        "Delete failed id={Id} PK={PK} Status={Status} Sub={Sub} RetryAfter={RetryAfter} RU={RU} ActivityId={ActivityId}",
                        item.id, item.RoundFileHash, (int)ex.StatusCode, (int)ex.SubStatusCode,
                        ex.RetryAfter, ex.RequestCharge, ex.ActivityId);
                }
                finally
                {
                    _writeLimiter.Release();
                }
            });

            await Task.WhenAll(tasks);
            sw.Stop();

            _logger.LogInformation(
                "Delete batch: requested={Requested} deleted={Deleted} notFound={NotFound} failed={Failed} throttled={Throttled} RU={RU} WallMs={Wall}",
                itemsToDelete.Count, deleted, notFound, failed, throttled, ruCharge, sw.ElapsedMilliseconds);

            return true;
        }

        // Replaces LogCosmosRequestInfo, which built a full StackTrace on every page and
        // every batch and could emit several identical lines per call (no break in the
        // frame loop). Logs Info only when an op is genuinely slow or RU-heavy, Debug
        // otherwise, so the fast path costs nothing in production.
        private void LogOp(string op, string? partitionKey, long wallMs, double ru, int items, int pages)
        {
            bool slow = wallMs >= 1000 || ru >= 100;

            if (!slow && !_logger.IsEnabled(LogLevel.Debug))
            {
                return;
            }

            _logger.Log(slow ? LogLevel.Information : LogLevel.Debug,
                "Cosmos {Op}{Slow} PK={PK} WallMs={Wall} RU={RU} Items={Items} Pages={Pages}",
                op,
                slow ? " SLOW" : string.Empty,
                string.IsNullOrEmpty(partitionKey) ? "<none>" : partitionKey,
                wallMs, ru, items, pages);
        }

        // Preserves StatusCode / SubStatusCode / RetryAfter / RequestCharge, all of which
        // the old "throw new Exception(ex.Message)" wrapper discarded - a 429 storm used to
        // be indistinguishable from a clean run.
        private void LogCosmosException(string op, CosmosException ex, string? partitionKey, string? id)
        {
            var level = ex.StatusCode == HttpStatusCode.TooManyRequests
                ? LogLevel.Warning
                : LogLevel.Error;

            _logger.Log(level,
                "CosmosException Op={Op} id={Id} PK={PK} Status={Status} Sub={Sub} ActivityId={ActivityId} RU={RU} RetryAfter={RetryAfter} Message={Message}",
                op,
                string.IsNullOrEmpty(id) ? "<unknown>" : id,
                string.IsNullOrEmpty(partitionKey) ? "<unknown>" : partitionKey,
                (int)ex.StatusCode, (int)ex.SubStatusCode, ex.ActivityId,
                ex.RequestCharge, ex.RetryAfter, ex.Message);
        }
    }

    public static class ServiceCollectionExtensions
    {
        public static void AddCosmosDbServices(this IServiceCollection services, CosmosDbOptions cosmosDbOptions)
        {
            // Deployed, Functions serves managed-identity tokens via IDENTITY_ENDPOINT, not
            // IMDS - but DefaultAzureCredential probes 169.254.169.254 anyway and the failed
            // probe shows up as a Faulted dependency in App Insights. Naming the credential
            // skips the chain walk entirely. DefaultAzureCredential stays for local dev,
            // where ManagedIdentityCredential has nothing to talk to.
#if DEBUG
            TokenCredential credential = new DefaultAzureCredential();
#else
            TokenCredential credential = new ManagedIdentityCredential(ManagedIdentityId.SystemAssigned);
#endif

            // Register CosmosClient as a singleton using RBAC
            services.AddSingleton(s => new CosmosClient(cosmosDbOptions.Endpoint, credential,
                new CosmosClientOptions
                {
                    ConnectionMode = ConnectionMode.Direct,
                    // Batch job, not request path - bulk trades per-op latency for throughput.
                    // Left off: App Insights shows zero 429s, so throttling isn't the bottleneck.
                    //AllowBulkExecution = true,
                    SerializerOptions = new CosmosSerializationOptions { IgnoreNullValues = true },
                    EnableTcpConnectionEndpointRediscovery = true,
                    ApplicationName = "GSProRoundStatsManagerFunctionApp"
                }));

            services.AddSingleton<ICosmosDbService<RoundStat>>(s => new CosmosDbService<RoundStat>(
                s.GetRequiredService<CosmosClient>(),
                cosmosDbOptions.DatabaseNameStats,
                "RoundStats",
                s.GetRequiredService<ILogger<CosmosDbService<RoundStat>>>()));
        }
    }
}