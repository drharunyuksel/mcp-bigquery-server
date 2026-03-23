#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListResourcesRequestSchema,
  ListToolsRequestSchema,
  ReadResourceRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { BigQuery } from '@google-cloud/bigquery';

import { promises as fs, constants as fsConstants } from 'fs';
import path from 'path';
import { runDailyScanIfNeeded } from './sensitive-field-scanner.js';

// Define configuration interface
interface ServerConfig {
  projectId: string;
  location?: string;
  keyFilename?: string;
  configFile?: string;
  maximumBytesBilled?: string;
}

// Define BigQuery configuration interface
interface BigQueryConfig {
  maximumBytesBilled: string;
  preventedFields: FieldRestrictionMap;
}

async function validateConfig(config: ServerConfig): Promise<void> {
  // Check if key file exists and is readable
  if (config.keyFilename) {
    const resolvedKeyPath = path.resolve(config.keyFilename);
    try {
      await fs.access(resolvedKeyPath, fsConstants.R_OK);
      // Update the config to use the resolved path
      config.keyFilename = resolvedKeyPath;
    } catch (error) {
      console.error('File access error details:', error);
      if (error instanceof Error) {
        const nodeError = error as NodeJS.ErrnoException;
        if (nodeError.code === 'EACCES') {
          throw new Error(`Permission denied accessing key file: ${resolvedKeyPath}. Please check file permissions.`);
        } else if (nodeError.code === 'ENOENT') {
          throw new Error(`Key file not found: ${resolvedKeyPath}. Please verify the file path.`);
        } else {
          throw new Error(`Unable to access key file: ${resolvedKeyPath}. Error: ${nodeError.message}`);
        }
      } else {
        throw new Error(`Unexpected error accessing key file: ${resolvedKeyPath}`);
      }
    }

    // Validate file contents
    try {
      const keyFileContent = await fs.readFile(config.keyFilename, 'utf-8');
      const keyData = JSON.parse(keyFileContent);
      
      // Basic validation of key file structure
      if (!keyData.type || keyData.type !== 'service_account' || !keyData.project_id) {
        throw new Error('Invalid service account key file format');
      }
    } catch (error) {
      if (error instanceof SyntaxError) {
        throw new Error('Service account key file is not valid JSON');
      }
      throw error;
    }
  }

  if (config.configFile) {
    const resolvedConfigPath = path.resolve(config.configFile);
    try {
      await fs.access(resolvedConfigPath, fsConstants.R_OK);
      config.configFile = resolvedConfigPath;
    } catch (error) {
      console.error('Configuration file access error details:', error);
      if (error instanceof Error) {
        const nodeError = error as NodeJS.ErrnoException;
        if (nodeError.code === 'EACCES') {
          throw new Error(`Permission denied accessing config file: ${resolvedConfigPath}. Please check file permissions.`);
        } else if (nodeError.code === 'ENOENT') {
          throw new Error(`Config file not found: ${resolvedConfigPath}. Please verify the file path.`);
        } else {
          throw new Error(`Unable to access config file: ${resolvedConfigPath}. Error: ${nodeError.message}`);
        }
      } else {
        throw new Error(`Unexpected error accessing config file: ${resolvedConfigPath}`);
      }
    }
  }

  // Validate project ID format (basic check)
  if (!/^[a-z0-9-]+$/.test(config.projectId)) {
    throw new Error('Invalid project ID format');
  }
}

function parseArgs(): ServerConfig {
  const args = process.argv.slice(2);
  const config: ServerConfig = {
    projectId: '',
    location: 'US' 
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (!arg.startsWith('--')) {
      throw new Error(`Invalid argument: ${arg}`);
    }

    const key = arg.slice(2);
    if (i + 1 >= args.length || args[i + 1].startsWith('--')) {
      throw new Error(`Missing value for argument: ${arg}`);
    }

    const value = args[++i];
    
    switch (key) {
      case 'project-id':
        config.projectId = value;
        break;
      case 'location':
        config.location = value;
        break;
      case 'key-file':
        config.keyFilename = value;
        break;
      case 'config-file':
        config.configFile = value;
        break;
      case 'maximum-bytes-billed':
        config.maximumBytesBilled = value;
        break;
      default:
        throw new Error(`Unknown argument: ${arg}`);
    }
  }

  if (!config.projectId) {
    throw new Error(
      "Missing required argument: --project-id\n" +
      "Usage: mcp-server-bigquery --project-id <project-id> [--location <location>] [--key-file <path-to-key-file>] [--config-file <path-to-config-file>] [--maximum-bytes-billed <maximum-bytes>]"
    );
  }

  return config;
}

const server = new Server(
  {
    name: "mcp-server/bigquery",
    version: "0.1.0",
  },
  {
    capabilities: {
      resources: {},
      tools: {},
    },
  },
);

let config: ServerConfig;
let bigquery: BigQuery;
let resourceBaseUrl: URL;

type FieldRestrictionMap = Record<string, string[]>;
let bigqueryConfig: BigQueryConfig;

// Aggregation functions that allow restricted columns when used as direct arguments.
const AGGREGATE_FUNCTIONS = ["count", "countif", "avg", "sum", "min", "max"];

interface StarUsage {
  qualifier?: string;
  exceptColumns: Set<string>;
  exceptBareColumns: Set<string>;
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

async function loadConfiguration(configFile?: string): Promise<BigQueryConfig> {
  const explicitPathProvided = Boolean(configFile);
  const resolvedPath = configFile
    ? path.resolve(configFile)
    : path.resolve(process.cwd(), 'config.json');

  let fileContents: string;

  try {
    fileContents = await fs.readFile(resolvedPath, 'utf-8');
  } catch (error) {
    const nodeError = error as NodeJS.ErrnoException;
    if (nodeError?.code === 'ENOENT') {
      if (explicitPathProvided) {
        throw new Error(`Config file not found: ${resolvedPath}`);
      }
      // No config file found and none explicitly requested — use defaults
      console.error('No config.json found, using defaults (1GB query limit, no field restrictions).');
      return {
        maximumBytesBilled: '1000000000',
        preventedFields: {},
      };
    }
    throw new Error(`Unable to read config file ${resolvedPath}: ${nodeError?.message ?? 'Unknown error'}`);
  }

  try {
    const parsed = JSON.parse(fileContents);
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new Error('Config file must be a JSON object with maximumBytesBilled and preventedFields properties.');
    }

    const config = parsed as Record<string, unknown>;

    // Validate maximumBytesBilled
    if (!config.maximumBytesBilled || typeof config.maximumBytesBilled !== 'string') {
      throw new Error('Config file must contain a maximumBytesBilled string property.');
    }

    // Validate preventedFields
    if (!config.preventedFields || typeof config.preventedFields !== 'object' || Array.isArray(config.preventedFields)) {
      throw new Error('Config file must contain a preventedFields object mapping table names to arrays of column names.');
    }

    const normalized: FieldRestrictionMap = {};

    for (const [tableName, fields] of Object.entries(config.preventedFields as Record<string, unknown>)) {
      if (!Array.isArray(fields) || fields.some((field) => typeof field !== 'string')) {
        throw new Error(`Invalid field list for table "${tableName}". Each table value must be an array of column name strings.`);
      }

      normalized[tableName.toLowerCase()] = (fields as string[]).map((field) => field.toLowerCase());
    }

    // Use CLI parameter if provided, otherwise use config file value
    const finalMaximumBytesBilled = config.maximumBytesBilled || config.maximumBytesBilled as string;

    const result: BigQueryConfig = {
      maximumBytesBilled: finalMaximumBytesBilled,
      preventedFields: normalized,
    };

    const source = config.maximumBytesBilled ? `CLI parameter` : `${resolvedPath}`;
    console.error(`Using maximumBytesBilled=${result.maximumBytesBilled} from ${source}, field restrictions for ${Object.keys(normalized).length} tables from ${resolvedPath}`);
    return result;
  } catch (error) {
    if (error instanceof SyntaxError) {
      throw new Error('Config file is not valid JSON');
    }
    throw error;
  }
}

function buildTableAliasMap(sql: string): Record<string, string> {
  const aliasMap: Record<string, string> = {};
  const tableRefPattern = /\b(?:from|join)\s+([a-z0-9_.]+)(?:\s+(?:as\s+)?([a-z0-9_]+))?/g;
  let match: RegExpExecArray | null;

  while ((match = tableRefPattern.exec(sql)) !== null) {
    const [, tableName, alias] = match;
    if (alias) {
      aliasMap[alias] = tableName;
    }
  }

  return aliasMap;
}

function referencesSameTable(candidate: string, expected: string): boolean {
  if (candidate === expected) {
    return true;
  }
  if (candidate.endsWith(`.${expected}`)) {
    return true;
  }
  return expected.endsWith(`.${candidate}`);
}

function extractSelectClause(sql: string): string {
  const selectMatch = sql.match(/\bselect\b([\s\S]*?)\bfrom\b/);
  if (!selectMatch) {
    return '';
  }
  return selectMatch[1];
}

function parseExceptColumns(segment: string): { full: Set<string>; bare: Set<string> } {
  const full = new Set<string>();
  const bare = new Set<string>();

  segment
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean)
    .forEach((value) => {
      const cleaned = value.replace(/`/g, '');
      const normalized = cleaned.toLowerCase();
      full.add(normalized);

      const bareName = normalized.split('.').pop();
      if (bareName) {
        bare.add(bareName);
      }
    });

  return { full, bare };
}

function extractStarUsages(selectClause: string): StarUsage[] {
  const usages: StarUsage[] = [];
  if (!selectClause) {
    return usages;
  }

  const starPattern = /(?:\b([a-z0-9_.]+)\.)?\*\s*(?:except\s*\(([^)]+)\))?/g;
  let match: RegExpExecArray | null;

  while ((match = starPattern.exec(selectClause)) !== null) {
    const matchIndex = match.index ?? starPattern.lastIndex - match[0].length;
    const precedingIndex = matchIndex - 1;
    const charBefore = precedingIndex >= 0 ? selectClause[precedingIndex] : ' ';

    if (charBefore === '(') {
      // Likely part of COUNT(*) or similar aggregate; skip.
      continue;
    }

    if (matchIndex > 0 && !/[,\s]/.test(charBefore)) {
      // Skip tokens appearing in string literals or identifiers (e.g. '*_suffix').
      continue;
    }

    const qualifier = match[1]?.toLowerCase();
    const exceptSegment = match[2];
    const exceptColumns = new Set<string>();
    const exceptBareColumns = new Set<string>();

    if (exceptSegment) {
      const parsed = parseExceptColumns(exceptSegment);
      parsed.full.forEach((value) => exceptColumns.add(value));
      parsed.bare.forEach((value) => exceptBareColumns.add(value));
    }

    usages.push({ qualifier, exceptColumns, exceptBareColumns });
  }

  return usages;
}

function starUsageCoversField(
  usage: StarUsage,
  field: string,
  tableName: string,
  aliasToTableMap: Record<string, string>,
): boolean {
  if (usage.exceptBareColumns.has(field)) {
    return true;
  }

  if (usage.exceptColumns.has(field)) {
    return true;
  }

  if (usage.exceptColumns.has(`${tableName}.${field}`)) {
    return true;
  }

  if (!usage.qualifier) {
    return false;
  }

  const qualifier = usage.qualifier;
  if (usage.exceptColumns.has(`${qualifier}.${field}`)) {
    return true;
  }

  const resolvedTable = aliasToTableMap[qualifier];
  if (resolvedTable) {
    if (usage.exceptColumns.has(`${resolvedTable}.${field}`)) {
      return true;
    }
    if (usage.exceptBareColumns.has(field) && referencesSameTable(resolvedTable, tableName)) {
      return true;
    }
  }

  return false;
}

function enforceFieldRestrictions(sql: string, restrictions: FieldRestrictionMap): void {
  if (!Object.keys(restrictions).length) {
    return;
  }

  const normalizedSql = sql.replace(/`/g, '').toLowerCase();
  const blockedColumnsByTable: Record<string, Set<string>> = {};
  const aliasToTableMap = buildTableAliasMap(normalizedSql);
  const selectClause = extractSelectClause(normalizedSql);
  const starUsages = extractStarUsages(selectClause);

  for (const [tableName, restrictedFields] of Object.entries(restrictions)) {
    if (!normalizedSql.includes(tableName)) {
      continue;
    }

    const qualifiedStarPattern = new RegExp(`\\b${escapeRegExp(tableName)}\\.\\*`);
    const aliasStarDetected = Object.entries(aliasToTableMap).some(([alias, referencedTable]) => {
      if (!referencesSameTable(referencedTable, tableName)) {
        return false;
      }
      const aliasPattern = new RegExp(`\\b${escapeRegExp(alias)}\\.\\*`);
      return aliasPattern.test(normalizedSql);
    });

    const relevantStarUsages = starUsages.filter((usage) => {
      if (!usage.qualifier) {
        return true;
      }

      const resolved = aliasToTableMap[usage.qualifier] ?? usage.qualifier;
      return referencesSameTable(resolved, tableName);
    });

    const starViolation = ((): boolean => {
      if (!relevantStarUsages.length && !qualifiedStarPattern.test(normalizedSql) && !aliasStarDetected) {
        return false;
      }

      if (!relevantStarUsages.length) {
        return true;
      }

      return relevantStarUsages.some((usage) => {
        if (!usage.exceptBareColumns.size && !usage.exceptColumns.size) {
          return true;
        }

        return restrictedFields.some((field) => !starUsageCoversField(usage, field, tableName, aliasToTableMap));
      });
    })();

    if (starViolation) {
      if (!blockedColumnsByTable[tableName]) {
        blockedColumnsByTable[tableName] = new Set<string>();
      }
      for (const field of restrictedFields) {
        blockedColumnsByTable[tableName].add(field);
      }
    }

    for (const field of restrictedFields) {
      const fieldPattern = new RegExp(`\\b${escapeRegExp(field)}\\b`);
      const aggregatePattern = new RegExp(
        `\\b(?:${AGGREGATE_FUNCTIONS.join('|')})\\s*\\(\\s*(?:distinct\\s+)?(?:[\\w$]+\\.)*${escapeRegExp(field)}\\s*\\)`,
        'g',
      );

      const sqlWithoutAggregatedUsage = normalizedSql.replace(aggregatePattern, '');

      if (fieldPattern.test(sqlWithoutAggregatedUsage)) {
        if (!blockedColumnsByTable[tableName]) {
          blockedColumnsByTable[tableName] = new Set<string>();
        }
        blockedColumnsByTable[tableName].add(field);
      }
    }
  }

  if (Object.keys(blockedColumnsByTable).length) {
    const messageDetails = Object.entries(blockedColumnsByTable)
      .map(([table, fields]) => {
        const columns = Array.from(fields).map((column) => `"${column}"`).join(', ');
        return `table "${table}" columns ${columns}`;
      })
      .join('; ');

    const allowedAggregates = `[${AGGREGATE_FUNCTIONS.map((fn) => `"${fn}"`).join(', ')}]`;

    throw new Error(`Restricted fields detected for ${messageDetails}. You can only use these columns inside ${allowedAggregates} aggregate functions or exclude them with SELECT * EXCEPT (...).`);
  }
}

try {
  config = parseArgs();
  await validateConfig(config);
  
  console.error(`Initializing BigQuery with project ID: ${config.projectId} and location: ${config.location}`);
  
  const bigqueryOptions: {
    projectId: string;
    keyFilename?: string;
  } = {
    projectId: config.projectId
  };
  
  if (config.keyFilename) {
    console.error(`Using service account key file: ${config.keyFilename}`);
    bigqueryOptions.keyFilename = config.keyFilename;
  }
  
  bigquery = new BigQuery(bigqueryOptions);
  resourceBaseUrl = new URL(`bigquery://${config.projectId}`);

  // Run daily sensitive field scan if config is stale
  const configFilePath = config.configFile
    ? path.resolve(config.configFile)
    : path.resolve(process.cwd(), 'config.json');
  try {
    await runDailyScanIfNeeded(bigquery, configFilePath);
  } catch (error) {
    console.error('Warning: daily sensitive field scan failed, using existing config.', error);
  }

  bigqueryConfig = await loadConfiguration(config.configFile);
} catch (error) {
  console.error('Initialization error:', error);
  process.exit(1);
}

const SCHEMA_PATH = "schema";

function qualifyTablePath(sql: string, projectId: string): string {
  // Match FROM INFORMATION_SCHEMA.TABLES or FROM dataset.INFORMATION_SCHEMA.TABLES
  const unqualifiedPattern = /FROM\s+(?:(\w+)\.)?INFORMATION_SCHEMA\.TABLES/gi;
  return sql.replace(unqualifiedPattern, (match, dataset) => {
    if (dataset) {
      return `FROM \`${projectId}.${dataset}.INFORMATION_SCHEMA.TABLES\``;
    }
    throw new Error("Dataset must be specified when querying INFORMATION_SCHEMA (e.g. dataset.INFORMATION_SCHEMA.TABLES)");
  });
}

server.setRequestHandler(ListResourcesRequestSchema, async () => {
  try {
    console.error('Fetching datasets...');
    const [datasets] = await bigquery.getDatasets();
    console.error(`Found ${datasets.length} datasets`);
    
    const resources = [];

    for (const dataset of datasets) {
      console.error(`Processing dataset: ${dataset.id}`);
      const [tables] = await dataset.getTables();
      console.error(`Found ${tables.length} tables and views in dataset ${dataset.id}`);
      
      for (const table of tables) {
        // Get the metadata to check if it's a table or view
        const [metadata] = await table.getMetadata();
        const resourceType = metadata.type === 'VIEW' ? 'view' : 'table';
        
        resources.push({
          uri: new URL(`${dataset.id}/${table.id}/${SCHEMA_PATH}`, resourceBaseUrl).href,
          mimeType: "application/json",
          name: `"${dataset.id}.${table.id}" ${resourceType} schema`,
        });
      }
    }

    console.error(`Total resources found: ${resources.length}`);
    return { resources };
  } catch (error) {
    console.error('Error in ListResourcesRequestSchema:', error);
    throw error;
  }
});

server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
  const resourceUrl = new URL(request.params.uri);
  const pathComponents = resourceUrl.pathname.split("/");
  const schema = pathComponents.pop();
  const tableId = pathComponents.pop();
  const datasetId = pathComponents.pop();

  if (schema !== SCHEMA_PATH) {
    throw new Error("Invalid resource URI");
  }

  const dataset = bigquery.dataset(datasetId!);
  const table = dataset.table(tableId!);
  const [metadata] = await table.getMetadata();

  return {
    contents: [
      {
        uri: request.params.uri,
        mimeType: "application/json",
        text: JSON.stringify(metadata.schema.fields, null, 2),
      },
    ],
  };
});

server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: [
      {
        name: "query",
        description: "Run a read-only BigQuery SQL query",
        inputSchema: {
          type: "object",
          properties: {
            sql: { type: "string" },
          },
        },
      },
    ],
  };
});

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  if (request.params.name === "query") {
    let sql = request.params.arguments?.sql as string;
    
    // Validate read-only query
    const forbiddenPattern = /\b(INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|MERGE|TRUNCATE|GRANT|REVOKE|EXECUTE|BEGIN|COMMIT|ROLLBACK)\b/i;
    if (forbiddenPattern.test(sql)) {
      throw new Error('Only READ operations are allowed');
    }    

    try {
      // Qualify INFORMATION_SCHEMA queries
      if (sql.toUpperCase().includes('INFORMATION_SCHEMA')) {
        sql = qualifyTablePath(sql, config.projectId);
      }

      enforceFieldRestrictions(sql, bigqueryConfig.preventedFields);

      const [rows] = await bigquery.query({
        query: sql,
        location: config.location,
        maximumBytesBilled: bigqueryConfig.maximumBytesBilled,
      });

      return {
        content: [{ type: "text", text: JSON.stringify(rows, null, 2) }],
        isError: false,
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'An unknown error occurred while executing the query.';
      console.error('Query tool error:', error);

      if (error instanceof Error && message.includes('restricted in this tool')) {
        return {
          content: [{ type: "text", text: message }],
          isError: false,
        };
      }

      return {
        content: [{ type: "text", text: message }],
        isError: true,
      };
    }
  }
  throw new Error(`Unknown tool: ${request.params.name}`);
});

async function runServer() {
  try {
    const transport = new StdioServerTransport();
    await server.connect(transport);
    console.error('BigQuery MCP server running on stdio');
  } catch (error: unknown) {
    if (error instanceof Error) {
      console.error('Error:', error.message);
    } else {
      console.error('An unknown error occurred:', error);
    }
    process.exit(1);
  }
}

runServer().catch(console.error);
