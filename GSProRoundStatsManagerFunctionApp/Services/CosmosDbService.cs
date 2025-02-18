using GSProRoundStatsManagerFunctionApp.ConfigurationOptions;
using GSProRoundStatsManagerFunctionApp.Models.Stats;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Linq.Expressions;

namespace GSProRoundStatsManagerFunctionApp.Services
{
    public interface ICosmosDbService<T> where T : class
    {
        Task<IEnumerable<T>> ReadFeedAsync(string partitionKey);
        Task<IEnumerable<T>> QueryItemsAsync(QueryDefinition query, string? partitionKey);
        Task<IEnumerable<T>> QueryItemsLinqAsync(Expression<Func<T, bool>> predicate);
        Task<T?> GetItemAsync(string id, string partitionKey);
        Task<T> CreateItemAsync(T item, string partitionKey);
        Task<T> UpsertItemAsync(string id, T item, string partitionKey);
        Task<T> PatchItemAsync(string id, string partitionKey, IReadOnlyList<PatchOperation> patchOperations);
        Task<bool> DeleteItemAsync(string id, string partitionKey);
        Task<bool> DeleteItemsInParallelAsync(List<RoundStat> itemsToDelete);
    }

    public class CosmosDbService<T> : ICosmosDbService<T> where T : class
    {
        private readonly Container _container;
        private readonly ILogger<CosmosDbService<T>> _logger;

        public CosmosDbService(CosmosClient cosmosClient, string databaseName, string containerName, ILogger<CosmosDbService<T>> logger)
        {
            _container = cosmosClient.GetContainer(databaseName, containerName);
            _logger = logger;
        }

        // ReadFeed items
        public async Task<IEnumerable<T>> ReadFeedAsync(string partitionKey)
        {
            try
            {
                FeedIterator<T> iterator = _container.GetItemQueryIterator<T>(
                    requestOptions: new QueryRequestOptions
                    {
                        PartitionKey = new PartitionKey(partitionKey),
                        MaxItemCount = -1
                    });

                List<T> results = new List<T>();
                while (iterator.HasMoreResults)
                {
                    var response = await iterator.ReadNextAsync();
                    LogCosmosRequestInfo(response.Count, response.RequestCharge);
                    results.AddRange(response.ToList());
                }

                return results;
            }
            catch (CosmosException ex)
            {
                _logger.LogError($"CosmosException: StatusCode={ex.StatusCode}, Message={ex.Message}, ActivityId={ex.ActivityId}");
                throw new Exception($"Error occurred: {ex.Message}", ex);
            }
        }


        // Query items
        public async Task<IEnumerable<T>> QueryItemsAsync(QueryDefinition queryDefinition, string? partitionKey)
        {
            try
            {
                FeedIterator<T> queryIterator = _container.GetItemQueryIterator<T>(queryDefinition, null,
                    partitionKey == null ? new QueryRequestOptions()
                    {
                        MaxItemCount = -1
                    } : new QueryRequestOptions()
                    {
                        PartitionKey = new PartitionKey(partitionKey),
                        MaxItemCount = -1
                    });

                List<T> results = new List<T>();
                while (queryIterator.HasMoreResults)
                {
                    var response = await queryIterator.ReadNextAsync();
                    LogCosmosRequestInfo(response.Count, response.RequestCharge);
                    results.AddRange(response.ToList());
                }

                return results;
            }
            catch (CosmosException ex)
            {
                _logger.LogError($"CosmosException: StatusCode={ex.StatusCode}, Message={ex.Message}, ActivityId={ex.ActivityId}");
                throw new Exception($"Error occurred: {ex.Message}", ex);
            }
        }

        // Query items using LINQ
        public async Task<IEnumerable<T>> QueryItemsLinqAsync(Expression<Func<T, bool>> predicate)
        {
            try
            {
                IQueryable<T> query = _container.GetItemLinqQueryable<T>();
                var matches = query.Where(predicate);

                using FeedIterator<T> linqFeed = matches.ToFeedIterator();

                List<T> results = new List<T>();
                while (linqFeed.HasMoreResults)
                {
                    var response = await linqFeed.ReadNextAsync();
                    LogCosmosRequestInfo(response.Count, response.RequestCharge);
                    results.AddRange(response.ToList());
                }

                return results;
            }
            catch (CosmosException ex)
            {
                _logger.LogError($"CosmosException: StatusCode={ex.StatusCode}, Message={ex.Message}, ActivityId={ex.ActivityId}");
                throw new Exception($"Error occurred: {ex.Message}", ex);
            }
        }

        // Read/Get an item by id
        public async Task<T?> GetItemAsync(string id, string partitionKey)
        {
            try
            {
                var response = await _container.ReadItemAsync<T>(id, new PartitionKey(partitionKey));
                LogCosmosRequestInfo(1, response.RequestCharge);
                return response.Resource;
            }
            catch (CosmosException ex)
            {
                if (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                    return null;
                else
                {
                    _logger.LogError($"CosmosException: StatusCode={ex.StatusCode}, Message={ex.Message}, ActivityId={ex.ActivityId}");
                    throw new Exception($"Error occurred: {ex.Message}", ex);
                }
            }
        }

        // Create/Insert an item
        public async Task<T> CreateItemAsync(T item, string partitionKey)
        {
            try
            {
                var response = await _container.CreateItemAsync(item, new PartitionKey(partitionKey));
                LogCosmosRequestInfo(1, response.RequestCharge);
                return response.Resource;
            }
            catch (CosmosException ex)
            {
                _logger.LogError($"CosmosException: StatusCode={ex.StatusCode}, Message={ex.Message}, ActivityId={ex.ActivityId}");
                throw new Exception($"Error occurred: {ex.Message}", ex);
            }
        }

        // Update an item
        public async Task<T> UpsertItemAsync(string id, T item, string partitionKey)
        {
            try
            {
                var response = await _container.UpsertItemAsync(item, new PartitionKey(partitionKey));
                LogCosmosRequestInfo(1, response.RequestCharge);
                return response.Resource;
            }
            catch (CosmosException ex)
            {
                _logger.LogError($"CosmosException: StatusCode={ex.StatusCode}, Message={ex.Message}, ActivityId={ex.ActivityId}");
                throw new Exception($"Error occurred: {ex.Message}", ex);
            }
        }

        // Patch an item
        public async Task<T> PatchItemAsync(string id, string partitionKey, IReadOnlyList<PatchOperation> patchOperations)
        {
            try
            {
                var response = await _container.PatchItemAsync<T>(id, new PartitionKey(partitionKey), patchOperations);
                LogCosmosRequestInfo(1, response.RequestCharge);
                return response.Resource;
            }
            catch (CosmosException ex)
            {
                _logger.LogError($"CosmosException: StatusCode={ex.StatusCode}, Message={ex.Message}, ActivityId={ex.ActivityId}");
                throw new Exception($"Error occurred: {ex.Message}", ex);
            }
        }

        // Delete an item by id
        public async Task<bool> DeleteItemAsync(string id, string partitionKey)
        {
            try
            {
                var response = await _container.DeleteItemAsync<T>(id, new PartitionKey(partitionKey));
                LogCosmosRequestInfo(1, response.RequestCharge);
                return true;
            }
            catch (CosmosException ex)
            {
                _logger.LogError($"CosmosException: StatusCode={ex.StatusCode}, Message={ex.Message}, ActivityId={ex.ActivityId}");
                throw new Exception($"Error occurred: {ex.Message}", ex);
            }
        }

        // Delete an item by id
        public async Task<bool> DeleteItemsInParallelAsync(List<RoundStat> itemsToDelete)
        {
            try
            {
                object lockObject = new object();
                int recordCount = 0;
                double ruCharge = 0;
                int batchSize = 100;

                var batches = itemsToDelete
                    .Select((item, index) => new { item, index })
                    .GroupBy(x => x.index / batchSize)
                    .Select(g => g.Select(x => x.item).ToList())
                    .ToList();

                foreach (var batch in batches)
                {
                    var tasks = batch.Select(async item =>
                    {
                        var response = await _container.DeleteItemAsync<T>(item.id, new PartitionKey(item.RoundFileHash));
                        lock (lockObject)
                        {
                            recordCount++;
                            ruCharge += response.RequestCharge;
                        }
                    });

                    await Task.WhenAll(tasks);
                    LogCosmosRequestInfo(recordCount, ruCharge);
                }

                return true;
            }
            catch (CosmosException ex)
            {
                _logger.LogError($"CosmosException: StatusCode={ex.StatusCode}, Message={ex.Message}, ActivityId={ex.ActivityId}");
                throw new Exception($"Error occurred: {ex.Message}", ex);
            }
        }

        public void LogCosmosRequestInfo(int recordCount, double ruCharge)
        {
            if (ruCharge > 0) // Only log if the RU Charge is greater than 50
            {
                StackTrace stackTrace = new StackTrace(); // skipFrames: 1, fNeedFileInfo: true);

                foreach (StackFrame frame in stackTrace.GetFrames())
                {
                    var method = frame.GetMethod();

                    if (method.DeclaringType != null &&
                        method.IsFinal &&
                        method.DeclaringType.Namespace != null &&
                        !method.DeclaringType.Namespace.StartsWith("System") &&  // Filter external code
                        !method.DeclaringType.Namespace.StartsWith("Microsoft") &&
                        !method.DeclaringType.Namespace.StartsWith("Newtonsoft") &&
                        !method.DeclaringType.FullName.Contains("DisplayClass"))
                    {
                        if (ruCharge > 0)
                        {
                            _logger.LogInformation("CosmosDB Request Information - Declaring Type: {DeclaringType}, Record Count: {RecordCount}, RUs: {RUs}",
                                method.DeclaringType.FullName, recordCount, ruCharge);
                        }
                    }
                }
            }
        }
    }

    public static class ServiceCollectionExtensions
    {
        public static void AddCosmosDbServices(this IServiceCollection services, CosmosDbOptions cosmosDbOptions)
        {
            // Register CosmosClient as a singleton
            services.AddSingleton(s => new CosmosClient(cosmosDbOptions.Endpoint, cosmosDbOptions.PrimaryKey,
                new CosmosClientOptions
                {
                    SerializerOptions = new CosmosSerializationOptions()
                    {
                        IgnoreNullValues = true
                    }
                }));

            services.AddSingleton<ICosmosDbService<RoundStat>>(s => new CosmosDbService<RoundStat>(
                s.GetRequiredService<CosmosClient>(),
                cosmosDbOptions.DatabaseNameStats,
                "RoundStats",
                s.GetRequiredService<ILogger<CosmosDbService<RoundStat>>>()));
        }
    }
}
