import { buildMeta } from '../metadata.js';
import { buildCitation } from '../citation.js';
import { validateJurisdiction } from '../jurisdiction.js';
import type { Database } from '../db.js';

interface SchemeDetailsArgs {
  scheme_id: string;
  jurisdiction?: string;
}

export function handleGetSchemeDetails(db: Database, args: SchemeDetailsArgs) {
  const jv = validateJurisdiction(args.jurisdiction);
  if (!jv.valid) return jv.error;

  const scheme = db.get<{
    id: string; name: string; scheme_type: string; authority: string;
    status: string; start_date: string; description: string;
    eligibility_summary: string; application_window: string; jurisdiction: string;
  }>(
    'SELECT * FROM schemes WHERE id = ? AND jurisdiction = ?',
    [args.scheme_id, jv.jurisdiction]
  );

  if (!scheme) {
    return { error: 'not_found', message: `Scheme '${args.scheme_id}' not found. Use search_schemes to find available schemes.` };
  }

  const options = db.all<{
    id: string; code: string; name: string; payment_rate: number; payment_unit: string;
  }>(
    'SELECT id, code, name, payment_rate, payment_unit FROM scheme_options WHERE scheme_id = ? AND jurisdiction = ?',
    [args.scheme_id, jv.jurisdiction]
  );

  return {
    ...scheme,
    options_count: options.length,
    options,
    _meta: buildMeta(),
    _citation: buildCitation(
      `UK Scheme: ${scheme.name}`,
      `${scheme.name} — ${scheme.scheme_type} (${jv.jurisdiction})`,
      'get_scheme_details',
      { scheme_id: args.scheme_id },
    ),
  };
}
