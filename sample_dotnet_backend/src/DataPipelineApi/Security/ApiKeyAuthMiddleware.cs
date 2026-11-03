using System.Text.Json;
using DataPipelineApi.Options;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace DataPipelineApi.Security;

public class ApiKeyAuthMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<ApiKeyAuthMiddleware> _logger;

    public ApiKeyAuthMiddleware(RequestDelegate next, ILogger<ApiKeyAuthMiddleware> logger)
    {
        _next = next;
        _logger = logger;
    }

    public async Task InvokeAsync(HttpContext context, IOptions<SecurityOptions> securityOptions)
    {
        var options = securityOptions.Value;
        var path = context.Request.Path.Value ?? string.Empty;

        // Exempt non-API and documentation endpoints from authentication
        if (!path.StartsWith("/api", StringComparison.OrdinalIgnoreCase))
        {
            await _next(context);
            return;
        }

        // If no API key is configured or enforcement is disabled, allow requests (e.g. dev/CI)
        if (string.IsNullOrWhiteSpace(options.ApiKey))
        {
            await _next(context);
            return;
        }

        // Check for API key in custom header (e.g. X-API-Key) or Authorization Bearer header
        string? providedKey = null;

        if (context.Request.Headers.TryGetValue(options.HeaderName, out var headerVal))
        {
            providedKey = headerVal.FirstOrDefault();
        }
        else if (context.Request.Headers.TryGetValue("Authorization", out var authVal))
        {
            var authHeader = authVal.FirstOrDefault() ?? string.Empty;
            if (authHeader.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
            {
                providedKey = authHeader.Substring("Bearer ".Length).Trim();
            }
        }

        if (string.IsNullOrWhiteSpace(providedKey) || !string.Equals(providedKey, options.ApiKey, StringComparison.Ordinal))
        {
            _logger.LogWarning("Unauthorized access attempt to {Path} from {IP} - invalid or missing API key.", path, context.Connection.RemoteIpAddress);

            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
            context.Response.ContentType = "application/problem+json";

            var problem = new ProblemDetails
            {
                Status = StatusCodes.Status401Unauthorized,
                Title = "Unauthorized",
                Detail = $"Valid API key required in '{options.HeaderName}' or 'Authorization: Bearer <key>' header.",
                Instance = path
            };
            problem.Extensions["traceId"] = context.TraceIdentifier;

            await context.Response.WriteAsync(JsonSerializer.Serialize(problem));
            return;
        }

        await _next(context);
    }
}

