# BigQuery MCP Server

> **This is a fork of [ergut/mcp-bigquery-server](https://github.com/ergut/mcp-bigquery-server) with additional security features:** field-level access restrictions, an automated sensitive field scanner, and centralized billing controls. A PR is open upstream — until it's merged, use this fork to get these features.

<div align="center">
  <img src="assets/mcp-bigquery-server-logo.png" alt="BigQuery MCP Server Logo" width="400"/>
</div>

## What is this? 🤔

This is a server that lets your LLMs (like Claude) talk directly to your BigQuery data! Think of it as a friendly translator that sits between your AI assistant and your database, making sure they can chat securely and efficiently.

### Quick Example
```text
You: "What were our top 10 customers last month?"
Claude: *queries your BigQuery database and gives you the answer in plain English*
```

No more writing SQL queries by hand - just chat naturally with your data!

## How Does It Work? 🛠️

This server uses the Model Context Protocol (MCP), which is like a universal translator for AI-database communication. While MCP is designed to work with any AI model, right now it's available as a developer preview in Claude Desktop.

Here's all you need to do:
1. Set up authentication (see below)
2. Add your project details to Claude Desktop's config file
3. Start chatting with your BigQuery data naturally!

### What Can It Do? 📊

- Run SQL queries by just asking questions in plain English
- Access both tables and materialized views in your datasets
- Explore dataset schemas with clear labeling of resource types (tables vs views)
- Analyze data within configurable safe limits (configured via config.json)
- **Protect sensitive data** — define field-level access restrictions to prevent AI agents from reading PII, PHI, financial data, and secrets. The agent receives clear guidance on how to reformulate queries using aggregates or `EXCEPT` clauses, so it remains useful without exposing individual records.
- **Auto-discover sensitive fields** — automatically scan your entire BigQuery data warehouse for columns matching sensitive patterns (names, emails, SSNs, medical records, API keys, etc.) and add them to the restricted list. New tables and columns are protected automatically on each scan — no manual maintenance required.
- **Fully configurable** — everything is driven by `config.json`. Add your own detection patterns to match your organization's naming conventions (e.g., `%guardian_name%`, `%beneficiary%`), adjust scan frequency, set billing limits, and define per-table field restrictions. The scanner picks up your custom patterns on the next run and automatically protects any matching columns across all datasets.

## Quick Start 🚀

### Prerequisites
- Node.js 14 or higher
- Google Cloud project with BigQuery enabled
- Either Google Cloud CLI installed or a service account key file
- Any MCP-compatible AI client (Claude Desktop, Cursor, Windsurf, etc.)

> **Note:** The Smithery install and `npx @ergut/mcp-bigquery-server` commands install the **original upstream package** and will not include the security features in this fork. Use the setup below to get field restrictions and the sensitive field scanner.

### Step 1: Clone and build this fork

```bash
git clone https://github.com/drharunyuksel/mcp-bigquery-server
cd mcp-bigquery-server
npm install
npm run build
```

### Step 2: Authenticate with Google Cloud (choose one method)

- Using Google Cloud CLI (great for development):
  ```bash
  gcloud auth application-default login
  ```
- Using a service account (recommended for production):
  ```bash
  # Save your service account key file and use --key-file parameter
  # Never commit your service account key to version control
  ```

### Step 3: Add to your Claude Desktop config

Add this to your `claude_desktop_config.json`, pointing to your local build:

- Basic configuration:
  ```json
  {
    "mcpServers": {
      "bigquery": {
        "command": "node",
        "args": [
          "/path/to/your/clone/mcp-bigquery-server/dist/index.js",
          "--project-id",
          "your-project-id",
          "--location",
          "us-central1"
        ]
      }
    }
  }
  ```

- With service account and config file:
  ```json
  {
    "mcpServers": {
      "bigquery": {
        "command": "node",
        "args": [
          "/path/to/your/clone/mcp-bigquery-server/dist/index.js",
          "--project-id",
          "your-project-id",
          "--location",
          "us-central1",
          "--key-file",
          "/path/to/service-account-key.json",
          "--config-file",
          "/path/to/config.json"
        ]
      }
    }
  }
  ```

### Step 4: Start chatting!
Open Claude Desktop and start asking questions about your data.

### Configuration

The server supports an optional `config.json` file for advanced configuration. Without a config file, the server uses safe defaults (1GB query limit, no field restrictions). Place the file in the same directory where you run the server or specify its path with `--config-file`.

#### config.json Structure
```json
{
  "maximumBytesBilled": "1000000000",
  "preventedFields": {
    "healthcare.patients": ["first_name", "last_name", "ssn", "date_of_birth", "email"],
    "billing.transactions": ["credit_card_number", "bank_account"]
  },
  "sensitiveFieldPatterns": [
    "%first_name%", "%last_name%", "%email%",
    "%ssn%", "%date_of_birth%", "%password%"
  ],
  "sensitiveFieldScanFrequencyDays": 1
}
```

| Setting | Default | Description |
|---------|---------|-------------|
| `maximumBytesBilled` | `"1000000000"` (1GB) | Maximum bytes billed per query |
| `preventedFields` | `{}` | Table-to-columns mapping of restricted fields |
| `sensitiveFieldPatterns` | Built-in set | SQL LIKE patterns for auto-discovery |
| `sensitiveFieldScanFrequencyDays` | `1` | Days between auto-scans (`0` to disable) |

#### Command Line Arguments

- `--project-id`: (Required) Your Google Cloud project ID
- `--location`: (Optional) BigQuery location, defaults to 'US'
- `--key-file`: (Optional) Path to service account key JSON file
- `--config-file`: (Optional) Path to configuration file, defaults to 'config.json'
- `--maximum-bytes-billed`: (Optional) Override maximum bytes billed for queries, overrides config.json value

Example using service account:
```bash
npx @ergut/mcp-bigquery-server --project-id your-project-id --location europe-west1 --key-file /path/to/key.json --config-file /path/to/config.json --maximum-bytes-billed 2000000000
```

## Protecting Sensitive Data 🔒

Data warehouses often contain highly sensitive information — patient records, social security numbers, financial data, personal contact details, and authentication secrets. When an AI agent has direct access to query your BigQuery warehouse, **there is no human in the loop to prevent it from reading sensitive columns**. A simple query like `SELECT * FROM patients` could expose thousands of PII/PHI records in a single response.

This server gives administrators fine-grained control over which columns an AI agent can access, ensuring sensitive data stays protected while still allowing the AI to perform useful analytical queries on non-sensitive fields.

### Field-Level Access Restrictions

Define `preventedFields` in your config to block the AI agent from accessing specific columns:

```json
{
  "preventedFields": {
    "healthcare.patients": ["first_name", "last_name", "ssn", "date_of_birth", "email"],
    "billing.transactions": ["credit_card_number", "bank_account"]
  }
}
```

**What happens when the AI agent tries to access a restricted field:**

```sql
SELECT first_name, last_name, diagnosis FROM healthcare.patients
```

The server blocks the query and returns a clear, instructive error:

```
Restricted fields detected for table "healthcare.patients" columns "first_name", "last_name".
You can only use these columns inside ["count", "countif", "avg", "sum", "min", "max"]
aggregate functions or exclude them with SELECT * EXCEPT (...).
```

The AI agent learns from this error and adjusts its queries automatically. It can still run analytical queries that don't expose individual sensitive values:

```sql
-- Allowed: aggregate functions don't expose individual values
SELECT COUNT(first_name) AS patient_count, diagnosis
FROM healthcare.patients
GROUP BY diagnosis

-- Allowed: explicitly excluding restricted fields
SELECT * EXCEPT(first_name, last_name, ssn, date_of_birth, email)
FROM healthcare.patients
```

**Query pattern reference:**

| Query Pattern | Behavior |
|---|---|
| `SELECT restricted_col FROM table` | Blocked with error message |
| `SELECT * FROM table` | Blocked (would expose restricted fields) |
| `SELECT * EXCEPT(restricted_cols) FROM table` | Allowed |
| `COUNT(restricted_col)`, `AVG(...)`, `SUM(...)`, `MIN(...)`, `MAX(...)` | Allowed (aggregates don't expose individual values) |
| `SELECT non_restricted_col FROM table` | Allowed |

**Server-side logging:** Every blocked query is logged on the server side, giving administrators visibility into what the AI agent attempted to access:
```
Query tool error: Error: Restricted fields detected for table "healthcare.patients" columns "first_name", "last_name".
```

### Automated Sensitive Field Scanner

Manually listing every sensitive column across hundreds of tables is impractical. The server includes an automated scanner that discovers sensitive columns across **all** your BigQuery datasets by querying `INFORMATION_SCHEMA.COLUMNS` with configurable SQL LIKE patterns. Discovered fields are automatically added to `preventedFields` in your config.

#### How It Works

1. The scanner runs SQL LIKE pattern matching against all column names in your BigQuery project
2. Columns matching patterns like `%first_name%`, `%ssn%`, `%email%` are identified as sensitive
3. Discovered columns are merged into your config's `preventedFields`
4. The merge is **additive-only** — manually added restrictions are never removed

#### Auto-Scan on Server Startup

When the MCP server starts, it checks if the config file is stale based on `sensitiveFieldScanFrequencyDays`. If stale, it automatically scans and updates the config:

```
Config is stale (scan frequency: 1 day(s)), running sensitive field scan...
Scanning all datasets for sensitive fields...
Found 1166 sensitive column(s) across 278 table(s)
Scan complete: config updated with 278 tables.
```

This means **new tables with sensitive columns are automatically protected** without any manual configuration. As your data warehouse grows, the scanner keeps up.

#### Manual Scan via CLI

Run a scan on demand at any time:
```bash
npm run scan-fields -- --project-id your-project-id --config-file ./config.json
```

#### Custom Patterns for Your Organization

The default patterns cover common naming conventions (names, emails, SSNs, dates of birth, medical record numbers, insurance IDs, passwords, API keys, etc.), but every organization has its own. Add custom patterns to match your schema:

```json
{
  "sensitiveFieldPatterns": [
    "%first_name%", "%last_name%", "%email%", "%ssn%",
    "%date_of_birth%", "%password%", "%api_key%",
    "%guardian_name%",
    "%emergency_contact%",
    "%beneficiary%",
    "%next_of_kin%"
  ]
}
```

On the next auto-scan (or manual `npm run scan-fields`), the scanner picks up columns matching your new patterns and automatically adds them to `preventedFields`. As your data warehouse grows and new tables are added, any columns matching your patterns are **automatically protected** without manual intervention.

#### Scanner Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `sensitiveFieldPatterns` | Built-in set covering names, contacts, identity, insurance, and secrets | SQL LIKE patterns to match against column names |
| `sensitiveFieldScanFrequencyDays` | `1` (daily) | Days between automatic scans. Set `0` to disable auto-scanning. |

### Permissions Needed

You'll need one of these:
- `roles/bigquery.user` (recommended)
- OR both:
  - `roles/bigquery.dataViewer`
  - `roles/bigquery.jobUser`

## Developer Setup (Optional) 🔧

Want to customize or contribute? Here's how to set it up locally:

```bash
# Clone and install
git clone https://github.com/drharunyuksel/mcp-bigquery-server
cd mcp-bigquery-server
npm install

# Build
npm run build
```

Then update your Claude Desktop config to point to your local build:
```json
{
  "mcpServers": {
    "bigquery": {
      "command": "node",
      "args": [
        "/path/to/your/clone/mcp-bigquery-server/dist/index.js",
        "--project-id",
        "your-project-id",
        "--location",
        "us-central1",
        "--key-file",
        "/path/to/service-account-key.json",
        "--config-file",
        "/path/to/config.json",
        "--maximum-bytes-billed",
        "2000000000"
      ]
    }
  }
}
```

## Current Limitations ⚠️

- Works with any MCP-compatible AI client (Claude Desktop, Cursor, Windsurf, and others)
- Connections are limited to local MCP servers running on the same machine
- Queries are read-only with configurable processing limits (set in config.json)
- While both tables and views are supported, some complex view types might have limitations
- A config.json file is optional; without one the server uses safe defaults

## Support & Resources 💬

- 🐛 [Report issues](https://github.com/ergut/mcp-bigquery-server/issues)
- 💡 [Feature requests](https://github.com/ergut/mcp-bigquery-server/issues)
- 📖 [Documentation](https://github.com/ergut/mcp-bigquery-server)

## License 📝

MIT License - See [LICENSE](LICENSE) file for details.

## Author ✍️

Originally created by [Salih Ergüt](https://github.com/ergut). Forked and extended with security features by [Harun Yüksel](https://github.com/drharunyuksel).


## Version History 📋

See [CHANGELOG.md](CHANGELOG.md) for updates and version history.