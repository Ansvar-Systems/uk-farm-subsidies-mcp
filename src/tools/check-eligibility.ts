import { buildMeta } from '../metadata.js';
import { buildCitation } from '../citation.js';
import { validateJurisdiction } from '../jurisdiction.js';
import type { Database } from '../db.js';

interface EligibilityArgs {
  land_type?: string;
  current_practice?: string;
  farm_type?: string;
  jurisdiction?: string;
}

export function handleCheckEligibility(db: Database, args: EligibilityArgs) {
  const jv = validateJurisdiction(args.jurisdiction);
  if (!jv.valid) return jv.error;

  const allOptions = db.all<{
    id: string; scheme_id: string; code: string; name: string;
    payment_rate: number; payment_unit: string;
    eligible_land_types: string; requirements: string;
  }>(
    'SELECT * FROM scheme_options WHERE jurisdiction = ?',
    [jv.jurisdiction]
  );

  const matches = allOptions.filter(option => {
    let match = true;

    if (args.land_type && option.eligible_land_types) {
      match = match && option.eligible_land_types.toLowerCase().includes(args.land_type.toLowerCase());
    }

    if (args.current_practice && option.requirements) {
      match = match && option.requirements.toLowerCase().includes(args.current_practice.toLowerCase());
    }

    return match;
  });

  // Look up scheme names for matched options
  const results = matches.map(option => {
    const scheme = db.get<{ name: string }>(
      'SELECT name FROM schemes WHERE id = ?',
      [option.scheme_id]
    );
    return {
      option_id: option.id,
      option_code: option.code,
      option_name: option.name,
      scheme_id: option.scheme_id,
      scheme_name: scheme?.name ?? option.scheme_id,
      payment_rate: option.payment_rate,
      payment_unit: option.payment_unit,
      eligible_land_types: option.eligible_land_types,
      requirements: option.requirements,
    };
  });

  return {
    query: {
      land_type: args.land_type ?? null,
      current_practice: args.current_practice ?? null,
      farm_type: args.farm_type ?? null,
    },
    jurisdiction: jv.jurisdiction,
    matches_count: results.length,
    matches: results,
    note: results.length === 0
      ? 'No matching options found. Try broader search terms or use search_schemes for free-text search.'
      : 'Eligibility is indicative only. Check the full scheme manual for definitive criteria.',
    _meta: buildMeta(),
    _citation: buildCitation(
      `UK Subsidy Eligibility Check`,
      `Eligibility check: ${[args.land_type, args.farm_type].filter(Boolean).join(', ') || 'all'} (${jv.jurisdiction})`,
      'check_eligibility',
      { ...(args.land_type && { land_type: args.land_type }), ...(args.farm_type && { farm_type: args.farm_type }) },
    ),
  };
}
