using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Azure.Messaging.EventGrid;
using Azure.Messaging.EventGrid.SystemEvents;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Polly;
using Newtonsoft.Json.Linq;
using Stylelabs.M.Base.Querying;
using Stylelabs.M.Base.Querying.Filters;
using Stylelabs.M.Sdk.Contracts.Base;
using System.Collections.Generic;
using System.Web;
using System.Security.Cryptography;
using System.Text;
using Sitecore.CH.Base.Services;
using static Sitecore.CH.Implementation.Constants.PortalPageComponent;
using Stylelabs.M.Framework.Essentials.LoadConfigurations;
using Stylelabs.M.Framework.Essentials.LoadOptions;
using PortalPageProperties = Sitecore.CH.Implementation.Constants.PortalPageComponent.Properties;
using SettingProperties = Sitecore.CH.Implementation.Constants.PortalPageComponent.SettingProperties;

namespace Sitecore.CH.Implementation.AzFunctions.Functions
{
    public class H_HandleBlobCreatedOrUpdated
    {
        private readonly ILogger _logger;
        private readonly IConfiguration _configuration;
        private readonly IMClientFactory _mClientFactory;
        private readonly ILoggingContextService _loggingContextService;
        public const string FunctionName = "H_HandleBlobCreatedOrUpdated";

        public H_HandleBlobCreatedOrUpdated(ILogger<H_HandleBlobCreatedOrUpdated> logger, IConfiguration configuration, IMClientFactory mClientFactory, ILoggingContextService loggingContextService)
        {
            this._logger = logger;
            this._configuration = configuration;
            this._mClientFactory = mClientFactory;
            this._loggingContextService = loggingContextService;
        }

        [FunctionName(FunctionName)]
        public async Task Run(
            [EventGridTrigger] EventGridEvent eventGridEvent, ILogger log)
        {
            try
            {
                if (eventGridEvent.EventType == "Microsoft.Storage.BlobCreated")
                {
                    var blobCreatedEvent = eventGridEvent.Data.ToObjectFromJson<StorageBlobCreatedEventData>();

                    string blobUrl = blobCreatedEvent.Url;
                    string containerName = new Uri(blobUrl).Segments[1].TrimEnd('/');
                    string blobName = string.Join(string.Empty, new Uri(blobUrl).Segments.Skip(2));

                    log.LogInformation($"Processing blob created event for URL: {blobUrl}");

                    string connectionString = _configuration["Storage:ConnectionString"];
                    if (string.IsNullOrEmpty(connectionString))
                    {
                        log.LogError("Blob storage connection string is not configured");
                        return;
                    }

                    BlobServiceClient blobServiceClient = new BlobServiceClient(connectionString);
                    BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
                    BlobClient blobClient = containerClient.GetBlobClient(blobName);

                    string contentType = GetContentType(blobName);
                    string contentEncoding = "gzip";
                    string cacheControl = "max-age=86400, public";

                    BlobHttpHeaders headers = new BlobHttpHeaders
                    {
                        ContentType = contentType,
                        ContentEncoding = contentEncoding,
                        CacheControl = cacheControl
                    };

                    var retryPolicy = Policy
                        .Handle<Exception>()
                        .WaitAndRetryAsync(3, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (exception, timeSpan, retryCount, context) =>
                        {
                            log.LogWarning($"Retry {retryCount} encountered an error: {exception.Message}. Waiting {timeSpan} before next retry. Context: {context.OperationKey}");
                        });

                    await retryPolicy.ExecuteAsync(async () =>
                    {
                        await blobClient.SetHttpHeadersAsync(headers);
                    });

                    log.LogInformation($"Headers set for blob: {blobName}");

                    var metadata = new Dictionary<string, string>
                    {
                        { "processedBy", "H_HandleBlobCreatedOrUpdated" },
                        { "processedOn", DateTime.UtcNow.ToString("o") }
                    };

                    await blobClient.SetMetadataAsync(metadata);
                    log.LogInformation($"Metadata set for blob: {blobName}");

                    // Update external component version
                    string newVersion = GenerateVersionIdentifier();
                    log.LogInformation($"Generated new version identifier: {newVersion}");

                    var components = await GetExternalComponents(blobName, log);
                    await UpdateExternalComponent(components, newVersion, log);
                }
                else if (eventGridEvent.EventType == "Microsoft.Storage.BlobDeleted")
                {
                    log.LogInformation($"Blob deleted: {eventGridEvent.Data.ToString()}");
                }
            }
            catch (Exception ex)
            {
                log.LogError($"Unhandled exception: {ex.Message}", ex);
            }
        }

        private string GetContentType(string blobName)
        {
            if (blobName.EndsWith(".js.gz"))
            {
                return "application/javascript";
            }
            else if (blobName.EndsWith(".css.gz"))
            {
                return "text/css";
            }
            else if (blobName.EndsWith(".html.gz"))
            {
                return "text/html";
            }
            else if (blobName.EndsWith(".json.gz"))
            {
                return "application/json";
            }
            return "application/octet-stream"; // Default to binary stream if type is unknown
        }

        private async Task<List<IEntity>> GetExternalComponents(string fileName, ILogger log)
        {
            var components = new List<IEntity>();
            var componentType = await _mClientFactory.Client.Entities.GetAsync(Constants.PortalPageComponent.ComponentTypeIdentifiers.External);

            if (componentType == null)
            {
                log.LogWarning("No external component type found.");
                return components;
            }

            var query = new Query
            {
                Filter = new CompositeQueryFilter
                {
                    Children = new List<QueryFilter>
                    {
                        new DefinitionQueryFilter
                        {
                            Name = Constants.PortalPageComponent.DefinitionName
                        },

                        new RelationQueryFilter
                        {
                            Relation = Constants.PortalPageComponent.Relations.ComponentToPageComponent,
                            ParentId = componentType.Id
                        }
                    }
                }
            };

            var componentEntityIterator = _mClientFactory.Client.Querying.CreateEntityIterator(query,
                new EntityLoadConfiguration
                {
                    PropertyLoadOption = new PropertyLoadOption(PortalPageProperties.PageComponentSettings)
                });

            while (await componentEntityIterator.MoveNextAsync())
            {
                foreach (var component in componentEntityIterator.Current.Items)
                {
                    var settings = component.GetPropertyValue<JObject>(PortalPageProperties.PageComponentSettings);
                    var pathToken = settings?.SelectToken(SettingProperties.Path);
                    var pathTokenValue = pathToken?.ToString();

                    log.LogInformation($"Checking component {component.Id} with path {pathTokenValue}");

                    if (!string.IsNullOrWhiteSpace(pathTokenValue) && pathTokenValue.Contains(fileName))
                    {
                        log.LogInformation($"Component {component.Id} matches the file name.");
                        components.Add(component);
                    }
                }
            }

            return components;
        }

        private async Task UpdateExternalComponent(IEnumerable<IEntity> components, string newVersion, ILogger log)
        {
            var exceptions = new List<Exception>();
            foreach (var component in components)
            {
                try
                {
                    var settings = component.GetPropertyValue<JObject>(PortalPageProperties.PageComponentSettings);
                    var pathToken = settings.SelectToken(SettingProperties.Path);

                    var pathTokenValue = pathToken.ToString();
                    var uriBuilder = new UriBuilder(pathTokenValue)
                    {
                        Port = -1
                    };
                    var query = HttpUtility.ParseQueryString(uriBuilder.Query);

                    // Always update or add the version parameter
                    query[SettingProperties.PathVersion] = newVersion;
                    uriBuilder.Query = query.ToString();

                    log.LogInformation($"Updating component {component.Id} path from {pathTokenValue} to {uriBuilder.ToString()}");

                    pathToken.Replace(uriBuilder.ToString());

                    component.SetPropertyValue(PortalPageProperties.PageComponentSettings, settings);
                    await _mClientFactory.Client.Entities.SaveAsync(component);

                    log.LogInformation($"External component {component.Id} was updated. New path: {uriBuilder}");
                }
                catch (Exception ex)
                {
                    log.LogError($"Couldn't update component {component?.Id}: {ex.Message}");
                    exceptions.Add(new Exception($"Couldn't update {component?.Id} component", ex));
                }
            }

            if (exceptions.Any())
            {
                throw new AggregateException(exceptions);
            }
        }

        private string GenerateVersionIdentifier()
        {
            using (var rng = RandomNumberGenerator.Create())
            {
                byte[] randomBytes = new byte[4];
                rng.GetBytes(randomBytes);
                return BitConverter.ToString(randomBytes).Replace("-", "").ToLower();
            }
        }
    }
}
