import { buildMeta } from '../metadata.js';
import { buildCitation } from '../citation.js';
import { validateJurisdiction } from '../jurisdiction.js';
import type { Database } from '../db.js';

interface PaymentRatesArgs {
  scheme_id: string;
  option_id?: string;
  jurisdiction?: string;
}

export function handleGetPaymentRates(db: Database, args: PaymentRatesArgs) {
  const jv = validateJurisdiction(args.jurisdiction);
  if (!jv.valid) return jv.error;

  // Verify scheme exists
  const scheme = db.get<{ id: string; name: string }>(
    'SELECT id, name FROM schemes WHERE id = ? AND jurisdiction = ?',
    [args.scheme_id, jv.jurisdiction]
  );

  if (!scheme) {
    return { error: 'not_found', message: `Scheme '${args.scheme_id}' not found. Use search_schemes to find available schemes.` };
  }

  let sql = 'SELECT * FROM scheme_options WHERE scheme_id = ? AND jurisdiction = ?';
  const params: unknown[] = [args.scheme_id, jv.jurisdiction];

  if (args.option_id) {
    sql += ' AND id = ?';
    params.push(args.option_id);
  }

  sql += ' ORDER BY code';

  const options = db.all<{
    id: string; code: string; name: string; description: string;
    payment_rate: number; payment_unit: string; duration_years: number;
    eligible_land_types: string; stacking_rules: string;
  }>(sql, params);

  if (args.option_id && options.length === 0) {
    return { error: 'not_found', message: `Option '${args.option_id}' not found in scheme '${args.scheme_id}'.` };
  }

  return {
    scheme: scheme.name,
    scheme_id: scheme.id,
    jurisdiction: jv.jurisdiction,
    options_count: options.length,
    options: options.map(o => ({
      id: o.id,
      code: o.code,
      name: o.name,
      payment_rate: o.payment_rate,
      payment_unit: o.payment_unit,
      duration_years: o.duration_years,
      eligible_land_types: o.eligible_land_types,
      stacking_rules: o.stacking_rules,
    })),
    _meta: buildMeta(),
    _citation: buildCitation(
      `UK Scheme Payment: ${scheme.name}`,
      `Payment rates for ${scheme.name} (${jv.jurisdiction})`,
      'get_payment_rates',
      { scheme_id: args.scheme_id },
    ),
  };
}
