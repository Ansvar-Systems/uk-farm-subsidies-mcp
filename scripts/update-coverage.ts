/**
 * Regenerate data/coverage.json from the current database.
 * Usage: npm run coverage:update
 */

import { createDatabase } from '../src/db.js';
import { writeFileSync } from 'fs';

const db = createDatabase();

const schemes = db.get<{ c: number }>('SELECT count(*) as c FROM schemes')!.c;
const schemeOptions = db.get<{ c: number }>('SELECT count(*) as c FROM scheme_options')!.c;
const crossCompliance = db.get<{ c: number }>('SELECT count(*) as c FROM cross_compliance')!.c;
const fts = db.get<{ c: number }>('SELECT count(*) as c FROM search_index')!.c;
const lastIngest = db.get<{ value: string }>('SELECT value FROM db_metadata WHERE key = ?', ['last_ingest']);

db.close();

const coverage = {
  mcp_name: 'Farm Subsidies MCP',
  jurisdiction: 'GB',
  build_date: lastIngest?.value ?? new Date().toISOString().split('T')[0],
  schemes,
  scheme_options: schemeOptions,
  cross_compliance: crossCompliance,
  fts_entries: fts,
};

writeFileSync('data/coverage.json', JSON.stringify(coverage, null, 2));
console.log('Updated data/coverage.json:', coverage);
