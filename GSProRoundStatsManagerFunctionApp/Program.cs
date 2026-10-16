using Azure.Extensions.AspNetCore.Configuration.Secrets;
using Azure.Identity;
using GSProRoundStatsManagerFunctionApp;
using GSProRoundStatsManagerFunctionApp.ConfigurationOptions;
using GSProRoundStatsManagerFunctionApp.Services;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Reflection;

bool isLocal = Environment.GetEnvironmentVariable("IS_RUNNING_LOCALLY") == "true";

var host = new HostBuilder()
    .ConfigureFunctionsWebApplication()
    .ConfigureAppConfiguration((hostContext, builder) => {
        var buildConfig = builder.Build();

        if (!isLocal)
        {
            builder.AddAzureKeyVault(
                new Uri($"https://{buildConfig["Azure:KeyVaultName"]}.vault.azure.net/"),
                new ManagedIdentityCredential(ManagedIdentityId.SystemAssigned),
                new KeyVaultSecretManager());
        }
        else
        {
            // local dev no Key Vault
            builder
                .SetBasePath(Environment.CurrentDirectory)
                .AddJsonFile("local.settings.json", true)
                .AddUserSecrets(Assembly.GetExecutingAssembly(), true)
                .AddEnvironmentVariables();
        }
    })
    .ConfigureServices((hostContext, services) => {
        services.AddApplicationInsightsTelemetryWorkerService();
        services.ConfigureFunctionsApplicationInsights();
        services.AddTransient<RoundStatsManager>();

        var cosmosDbOptions = hostContext.Configuration.GetSection("CosmosDb").Get<CosmosDbOptions>();
        services.AddCosmosDbServices(cosmosDbOptions);
    })
    .Build();

host.Run();