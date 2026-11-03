using System.ComponentModel.DataAnnotations;

namespace DataPipelineApi.Options;

public class SecurityOptions
{
    public const string SectionName = "Security";

    /// <summary>
    /// Master API Key for accessing pipeline orchestration and administrative endpoints.
    /// When empty or whitespace, API key verification is disabled (e.g. for development/testing).
    /// </summary>
    public string ApiKey { get; set; } = string.Empty;

    /// <summary>
    /// Name of the HTTP header used to pass the API key (defaults to X-API-Key).
    /// </summary>
    public string HeaderName { get; set; } = "X-API-Key";

    /// <summary>
    /// Whether API key enforcement is strictly required.
    /// </summary>
    public bool RequireApiKey { get; set; } = false;
}

