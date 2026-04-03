import { buildMeta } from '../metadata.js';
import type { Database } from '../db.js';

interface Source {
  name: string;
  authority: string;
  official_url: string;
  retrieval_method: string;
  update_frequency: string;
  license: string;
  coverage: string;
  last_retrieved?: string;
}

export function handleListSources(db: Database): { sources: Source[]; _meta: ReturnType<typeof buildMeta> } {
  const lastIngest = db.get<{ value: string }>('SELECT value FROM db_metadata WHERE key = ?', ['last_ingest']);

  const sources: Source[] = [
    {
      name: 'DEFRA SFI Guidance',
      authority: 'Department for Environment, Food and Rural Affairs',
      official_url: 'https://www.gov.uk/government/collections/sustainable-farming-incentive-guidance',
      retrieval_method: 'HTML_SCRAPE',
      update_frequency: 'quarterly',
      license: 'Open Government Licence v3',
      coverage: 'SFI scheme options, payment rates, eligibility criteria, and application guidance',
      last_retrieved: lastIngest?.value,
    },
    {
      name: 'Countryside Stewardship Higher Tier',
      authority: 'Department for Environment, Food and Rural Affairs / Natural England',
      official_url: 'https://www.gov.uk/government/collections/countryside-stewardship',
      retrieval_method: 'HTML_SCRAPE',
      update_frequency: 'quarterly',
      license: 'Open Government Licence v3',
      coverage: 'CS Higher Tier options, payment rates, eligibility criteria, and agreement guidance',
      last_retrieved: lastIngest?.value,
    },
    {
      name: 'RPA Scheme Manuals',
      authority: 'Rural Payments Agency',
      official_url: 'https://www.gov.uk/government/organisations/rural-payments-agency',
      retrieval_method: 'HTML_SCRAPE',
      update_frequency: 'quarterly',
      license: 'Open Government Licence v3',
      coverage: 'Scheme rules, application windows, payment schedules, and operational procedures',
      last_retrieved: lastIngest?.value,
    },
    {
      name: 'Cross-compliance GAEC/SMR',
      authority: 'Department for Environment, Food and Rural Affairs',
      official_url: 'https://www.gov.uk/guidance/cross-compliance',
      retrieval_method: 'HTML_SCRAPE',
      update_frequency: 'annual',
      license: 'Open Government Licence v3',
      coverage: 'Good Agricultural and Environmental Conditions (GAEC) and Statutory Management Requirements (SMR)',
      last_retrieved: lastIngest?.value,
    },
  ];

  return {
    sources,
    _meta: buildMeta({ source_url: 'https://www.gov.uk/government/collections/sustainable-farming-incentive-guidance' }),
  };
}
