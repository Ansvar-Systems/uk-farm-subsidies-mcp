import { describe, test, expect, beforeAll, afterAll } from 'vitest';
import { handleListSources } from '../../src/tools/list-sources.js';
import { createDatabase, type Database } from '../../src/db.js';
import { existsSync, unlinkSync } from 'fs';

const TEST_DB = 'tests/test-list-sources.db';

describe('list_sources tool', () => {
  let db: Database;

  beforeAll(() => {
    db = createDatabase(TEST_DB);
  });

  afterAll(() => {
    db.close();
    if (existsSync(TEST_DB)) unlinkSync(TEST_DB);
  });

  test('returns 4 data sources', () => {
    const result = handleListSources(db);
    expect(result.sources).toHaveLength(4);
  });

  test('each source has required fields', () => {
    const result = handleListSources(db);
    for (const source of result.sources) {
      expect(source).toHaveProperty('name');
      expect(source).toHaveProperty('authority');
      expect(source).toHaveProperty('official_url');
      expect(source).toHaveProperty('license');
    }
  });

  test('includes _meta', () => {
    const result = handleListSources(db);
    expect(result._meta).toHaveProperty('disclaimer');
  });
});
