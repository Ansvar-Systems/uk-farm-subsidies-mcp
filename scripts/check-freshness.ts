/**
 * Check data freshness and exit with non-zero code if stale.
 * Used by .github/workflows/check-freshness.yml
 */

import { createDatabase } from '../src/db.js';

const STALENESS_THRESHOLD_DAYS = 90;

const db = createDatabase();
const row = db.get<{ value: string }>('SELECT value FROM db_metadata WHERE key = ?', ['last_ingest']);
db.close();

if (!row?.value) {
  console.error('No last_ingest date found in database.');
  process.exit(1);
}

const lastIngest = new Date(row.value);
const now = new Date();
const daysSince = Math.floor((now.getTime() - lastIngest.getTime()) / (1000 * 60 * 60 * 24));

console.log(`Last ingest: ${row.value} (${daysSince} days ago)`);
console.log(`Threshold: ${STALENESS_THRESHOLD_DAYS} days`);

if (daysSince > STALENESS_THRESHOLD_DAYS) {
  console.error(`Data is STALE (${daysSince} days > ${STALENESS_THRESHOLD_DAYS} day threshold)`);
  process.exit(1);
}

console.log('Data is fresh.');
