import { buildMeta } from '../metadata.js';
import { buildCitation } from '../citation.js';
import { validateJurisdiction } from '../jurisdiction.js';
import type { Database } from '../db.js';

interface CrossComplianceArgs {
  requirement_id?: string;
  topic?: string;
  jurisdiction?: string;
}

export function handleGetCrossCompliance(db: Database, args: CrossComplianceArgs) {
  const jv = validateJurisdiction(args.jurisdiction);
  if (!jv.valid) return jv.error;

  if (args.requirement_id) {
    const req = db.get<{
      id: string; requirement: string; category: string;
      reference: string; description: string; applies_to: string; jurisdiction: string;
    }>(
      'SELECT * FROM cross_compliance WHERE id = ? AND jurisdiction = ?',
      [args.requirement_id, jv.jurisdiction]
    );

    if (!req) {
      return { error: 'not_found', message: `Requirement '${args.requirement_id}' not found.` };
    }

    return {
      ...req,
      _meta: buildMeta({ source_url: 'https://www.gov.uk/guidance/cross-compliance' }),
      _citation: buildCitation(
        `UK Cross-Compliance: ${req.requirement}`,
        `${req.requirement} — ${req.category} (${jv.jurisdiction})`,
        'get_cross_compliance',
        { requirement_id: args.requirement_id },
        'https://www.gov.uk/guidance/cross-compliance',
      ),
    };
  }

  if (args.topic) {
    const results = db.all<{
      id: string; requirement: string; category: string;
      reference: string; description: string; applies_to: string;
    }>(
      `SELECT * FROM cross_compliance
       WHERE jurisdiction = ? AND (
         LOWER(requirement) LIKE ? OR LOWER(description) LIKE ? OR LOWER(category) LIKE ?
       )`,
      [jv.jurisdiction, `%${args.topic.toLowerCase()}%`, `%${args.topic.toLowerCase()}%`, `%${args.topic.toLowerCase()}%`]
    );

    return {
      topic: args.topic,
      jurisdiction: jv.jurisdiction,
      results_count: results.length,
      results,
      _meta: buildMeta({ source_url: 'https://www.gov.uk/guidance/cross-compliance' }),
    };
  }

  // Return all cross-compliance requirements
  const all = db.all<{
    id: string; requirement: string; category: string;
    reference: string; applies_to: string;
  }>(
    'SELECT id, requirement, category, reference, applies_to FROM cross_compliance WHERE jurisdiction = ?',
    [jv.jurisdiction]
  );

  return {
    jurisdiction: jv.jurisdiction,
    results_count: all.length,
    results: all,
    _meta: buildMeta({ source_url: 'https://www.gov.uk/guidance/cross-compliance' }),
  };
}
