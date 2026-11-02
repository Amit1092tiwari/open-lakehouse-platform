using System.Threading;
using System.Threading.Tasks;
using DataPipelineApi.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DataPipelineApi.Controllers;

[ApiController]
[Route("api/warehouse")]
public class WarehouseController : ControllerBase
{
    private readonly IDbService _db;
    private readonly IBatchService _airflow;
    private readonly ILogger<WarehouseController> _logger;
    private readonly IConfiguration _config;

    public WarehouseController(
        IDbService db,
        IBatchService airflow,
        ILogger<WarehouseController> logger,
        IConfiguration config)
    {
        _db = db;
        _airflow = airflow;
        _logger = logger;
        _config = config;
    }

    /// <summary>Trigger the Snowflake/Postgres warehouse transformation DAG</summary>
    [HttpPost("transform")]
    public async Task<ActionResult> TriggerTransform(CancellationToken ct)
    {
        var runId = await _airflow.TriggerWarehouseTransformAsync(ct);
        _logger.LogInformation("Warehouse transform triggered: {RunId}", runId);
        return Ok(new
        {
            runId,
            message = "Warehouse transformation DAG triggered",
            target = IsSnowflakeConfigured() ? "snowflake" : "postgresql_fallback"
        });
    }

    /// <summary>Get daily order aggregations from the warehouse</summary>
    [HttpGet("aggregations/daily-orders")]
    public async Task<ActionResult> GetDailyOrderAggregations(CancellationToken ct)
    {
        try
        {
            const string sql = "SELECT date_key, total_orders, total_revenue, avg_order_value, unique_customers, max_order_value, min_order_value, updated_at FROM agg_daily_orders ORDER BY date_key DESC LIMIT 30";
            var rows = await _db.QueryPostgresAsync(sql, null, ct);
            return Ok(new
            {
                source = IsSnowflakeConfigured() ? "snowflake" : "postgresql",
                table = "agg_daily_orders",
                count = rows.Count,
                data = rows
            });
        }
        catch (System.Exception ex)
        {
            _logger.LogError(ex, "Failed to query warehouse daily orders aggregation");
            return StatusCode(503, new { error = "Warehouse database unavailable", message = ex.Message });
        }
    }

    /// <summary>Get pipeline run history from warehouse</summary>
    [HttpGet("pipeline-runs")]
    public async Task<ActionResult> GetPipelineRuns(CancellationToken ct)
    {
        try
        {
            const string sql = "SELECT dag_id, run_id, run_type, start_time, end_time, duration_seconds, status FROM fact_pipeline_runs ORDER BY start_time DESC LIMIT 20";
            var rows = await _db.QueryPostgresAsync(sql, null, ct);
            return Ok(new
            {
                source = IsSnowflakeConfigured() ? "snowflake" : "postgresql",
                table = "fact_pipeline_runs",
                count = rows.Count,
                data = rows
            });
        }
        catch (System.Exception ex)
        {
            _logger.LogError(ex, "Failed to query pipeline runs");
            return StatusCode(503, new { error = "Warehouse database unavailable", message = ex.Message });
        }
    }

    /// <summary>Health check for warehouse connectivity</summary>
    [HttpGet("health")]
    public async Task<ActionResult> WarehouseHealth(CancellationToken ct)
    {
        var snowflakeConfigured = IsSnowflakeConfigured();

        try
        {
            await _db.ExecutePostgresAsync("SELECT COUNT(*) FROM dim_date", ct);
            return Ok(new
            {
                status = "healthy",
                staging_db = "postgresql:connected",
                warehouse = snowflakeConfigured ? "snowflake:configured" : "postgresql:fallback",
                schema = "star_schema",
                snowflake = new
                {
                    configured = snowflakeConfigured,
                    account = _config["Snowflake:Account"] ?? "",
                    warehouse_name = _config["Snowflake:Warehouse"] ?? "PIPELINE_WH",
                    database = _config["Snowflake:Database"] ?? "PIPELINE_DB"
                }
            });
        }
        catch
        {
            return StatusCode(503, new
            {
                status = "unhealthy",
                staging_db = "postgresql:disconnected",
                warehouse = "unknown"
            });
        }
    }

    /// <summary>Get open-source warehouse status (PostgreSQL OLAP / DuckDB)</summary>
    [HttpGet("status")]
    public ActionResult GetWarehouseStatus()
    {
        return Ok(new
        {
            engine = "OpenSource_PostgreSQL_DuckDB",
            is_open_source = true,
            storage_format = "Columnar_Star_Schema",
            tables = new
            {
                dimensions = new[] { "dim_customers", "dim_date", "dim_products", "dim_devices" },
                facts = new[] { "fact_orders", "fact_sensor_readings", "fact_pipeline_runs" },
                aggregations = new[] { "agg_daily_orders", "agg_hourly_sensors" }
            },
            commercial_extension = new
            {
                name = "Snowflake",
                configured = IsSnowflakeConfigured()
            }
        });
    }

    /// <summary>Get Snowflake warehouse configuration status</summary>
    [HttpGet("snowflake/status")]
    public ActionResult GetSnowflakeStatus()
    {
        var configured = IsSnowflakeConfigured();
        return Ok(new
        {
            configured,
            account = configured ? _config["Snowflake:Account"] : null,
            warehouse_name = _config["Snowflake:Warehouse"] ?? "PIPELINE_WH",
            database = _config["Snowflake:Database"] ?? "PIPELINE_DB",
            schema_name = _config["Snowflake:Schema"] ?? "ANALYTICS",
            role = _config["Snowflake:Role"] ?? "PIPELINE_ROLE",
            tables = new
            {
                dimensions = new[] { "DIM_CUSTOMERS", "DIM_DATE", "DIM_PRODUCTS", "DIM_DEVICES" },
                facts = new[] { "FACT_ORDERS", "FACT_SENSOR_READINGS", "FACT_PIPELINE_RUNS" },
                aggregations = new[] { "AGG_DAILY_ORDERS", "AGG_HOURLY_SENSORS" },
                staging = new[] { "STG_ORDERS", "STG_SENSOR_READINGS" }
            }
        });
    }

    private bool IsSnowflakeConfigured()
    {
        var account = _config["Snowflake:Account"];
        return !string.IsNullOrWhiteSpace(account);
    }
}
