/**
 * UK Farm Subsidies MCP -- Data Ingestion Script
 *
 * Fetches all farming grant actions from the GOV.UK Content API, parses
 * payment rates, eligibility, and duration from the HTML body, then inserts
 * into SQLite. Also seeds cross-compliance (GAEC/SMR) reference data.
 *
 * Sources:
 *   1. GOV.UK Content API -- SFI farming_grant documents (102 actions)
 *   2. GOV.UK Content API -- CS Higher Tier farming_grant documents (132 actions)
 *   3. Cross-compliance GAEC/SMR -- curated reference data
 *
 * Usage:
 *   npm run ingest           -- full ingestion
 *   npm run ingest:fetch     -- fetch and cache JSON only (no DB writes)
 */

import { createDatabase, type Database } from '../src/db.js';
import { mkdirSync, writeFileSync, readFileSync, existsSync } from 'fs';
import { createHash } from 'crypto';

// ── Configuration ───────────────────────────────────────────────

const SEARCH_API = 'https://www.gov.uk/api/search.json';
const CONTENT_API = 'https://www.gov.uk/api/content';
const PAGE_SIZE = 50;
const API_DELAY_MS = 200;
const CACHE_DIR = 'data/.cache';
const FETCH_ONLY = process.argv.includes('--fetch-only');

// ── Types ───────────────────────────────────────────────────────

interface SearchResult {
  title: string;
  link: string;
  description: string;
  farming_grant_type: string;
  land_types?: string[];
  areas_of_interest?: string[];
}

interface ContentDetail {
  title: string;
  base_path: string;
  details: {
    body: string;
    metadata: {
      areas_of_interest?: string[];
      farming_grant_type?: string;
      land_types?: string[];
    };
  };
}

interface ParsedAction {
  code: string;
  name: string;
  description: string;
  paymentRate: number | null;
  paymentUnit: string;
  paymentRateRaw: string;
  durationYears: number | null;
  eligibleLandTypes: string[];
  requirements: string;
  link: string;
}

// ── API Fetching ────────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchJson<T>(url: string): Promise<T> {
  const res = await fetch(url, {
    headers: { 'Accept': 'application/json', 'User-Agent': 'uk-farm-subsidies-mcp/0.1.0 (data ingestion)' },
  });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} for ${url}`);
  }
  return res.json() as Promise<T>;
}

async function fetchAllSearchResults(
  grantType: string,
  label: string
): Promise<SearchResult[]> {
  const results: SearchResult[] = [];
  let start = 0;

  while (true) {
    const url =
      `${SEARCH_API}?filter_document_type=farming_grant` +
      `&filter_farming_grant_type=${grantType}` +
      `&count=${PAGE_SIZE}&start=${start}` +
      `&fields=title,link,description,farming_grant_type,land_types,areas_of_interest`;

    console.log(`  Fetching ${label} search results start=${start}...`);
    const data = await fetchJson<{ results: SearchResult[]; total: number }>(url);
    results.push(...data.results);

    if (results.length >= data.total || data.results.length === 0) break;
    start += PAGE_SIZE;
    await sleep(API_DELAY_MS);
  }

  console.log(`  Found ${results.length} ${label} actions.`);
  return results;
}

async function fetchContentDetail(link: string): Promise<ContentDetail | null> {
  // Check cache first
  const cacheKey = createHash('md5').update(link).digest('hex');
  const cachePath = `${CACHE_DIR}/${cacheKey}.json`;

  if (existsSync(cachePath)) {
    try {
      return JSON.parse(readFileSync(cachePath, 'utf-8')) as ContentDetail;
    } catch { /* fall through to fetch */ }
  }

  try {
    const detail = await fetchJson<ContentDetail>(`${CONTENT_API}${link}`);
    // Cache for re-runs
    writeFileSync(cachePath, JSON.stringify(detail));
    return detail;
  } catch (err) {
    console.warn(`  WARN: Failed to fetch ${link}: ${err instanceof Error ? err.message : String(err)}`);
    return null;
  }
}

// ── HTML Parsing ────────────────────────────────────────────────

function stripHtml(html: string): string {
  return html
    .replace(/<[^>]+>/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractSection(html: string, headingId: string): string {
  // Match <h2 id="heading-id">...</h2> then capture everything until the next <h2
  const pattern = new RegExp(
    `<h2[^>]*id=["']${headingId}["'][^>]*>.*?</h2>\\s*([\\s\\S]*?)(?=<h2[\\s>]|$)`,
    'i'
  );
  const match = html.match(pattern);
  return match ? match[1].trim() : '';
}

function parsePaymentRate(html: string): { rate: number | null; unit: string; raw: string } {
  const section = extractSection(html, 'how-much-youll-be-paid') ||
                  extractSection(html, 'how-much-youll-be-paid-');
  if (!section) return { rate: null, unit: '', raw: '' };

  const text = stripHtml(section);

  // Extract first pound amount: matches "£5", "£382", "£5.80", "£1,234"
  const amountMatch = text.match(/\u00a3([\d,]+(?:\.\d+)?)/);
  const rate = amountMatch ? parseFloat(amountMatch[1].replace(/,/g, '')) : null;

  // Extract the unit -- everything after the amount up to the next sentence or paragraph
  let unit = '';
  if (amountMatch) {
    // Get the text segment after the amount
    const afterAmount = text.slice(amountMatch.index! + amountMatch[0].length).trim();
    // Common patterns: "per hectare (ha) per year", "per 100 metres (m)", "per pond per year"
    const unitMatch = afterAmount.match(/^(per\s+[^.–\-]+?)(?:\s*[-–.]|\s*$)/i);
    if (unitMatch) {
      unit = unitMatch[1]
        .replace(/\s*\([^)]*\)\s*/g, ' ')  // Remove parenthetical abbreviations
        .replace(/\s+/g, ' ')
        .trim();
    } else {
      // Handle "for the assessment and plan per year" style (no leading "per")
      const forMatch = afterAmount.match(/^(for\s+[^.–\-]+?)(?:\s*[-–.]|\s*$)/i);
      if (forMatch) {
        unit = forMatch[1]
          .replace(/\s*\([^)]*\)\s*/g, ' ')
          .replace(/\s+/g, ' ')
          .trim();
      }
    }
  }

  // Clean up stray punctuation at the end
  unit = unit.replace(/\s*[()]+\s*$/, '').trim();

  return { rate, unit, raw: text };
}

function parseDuration(html: string): number | null {
  const section = extractSection(html, 'duration');
  if (!section) return null;

  const text = stripHtml(section);
  const yearMatch = text.match(/(\d+)\s*year/i);
  return yearMatch ? parseInt(yearMatch[1], 10) : null;
}

function parseDescription(html: string): string {
  // Try "Action's aim" section first
  let section = extractSection(html, 'actions-aim') ||
                extractSection(html, 'action-s-aim') ||
                extractSection(html, 'actions-aim-');
  if (section) return stripHtml(section).slice(0, 1000);

  // Fall back to the intro paragraph (everything before the first h2)
  const introMatch = html.match(/^([\s\S]*?)(?=<h2[\s>])/i);
  if (introMatch) {
    const intro = stripHtml(introMatch[1]);
    if (intro.length > 20) return intro.slice(0, 1000);
  }

  return '';
}

function parseLandTypes(html: string, metadata: ContentDetail['details']['metadata']): string[] {
  const types = new Set<string>();

  // From metadata
  if (metadata.land_types) {
    for (const lt of metadata.land_types) {
      types.add(lt.toLowerCase().replace(/-/g, ' '));
    }
  }

  // From the "Where you can do this action" / eligible land table
  const whereSection = extractSection(html, 'where-you-can-do-this-action') ||
                       extractSection(html, 'eligible-land');
  if (whereSection) {
    // Extract from table cells in the "Eligible land type" column
    const cellPattern = /<td[^>]*>(.*?)<\/td>/gi;
    let cellMatch: RegExpExecArray | null;
    let cellIndex = 0;
    while ((cellMatch = cellPattern.exec(whereSection)) !== null) {
      // First column in each row is the land type
      if (cellIndex % 3 === 0) {
        const cellText = stripHtml(cellMatch[1]).toLowerCase();
        // Skip ineligible entries, empty cells, and overly long text
        if (cellText && cellText.length < 80 && !cellText.includes('ineligible') && !cellText.includes('must not')) {
          types.add(cellText);
        }
      }
      cellIndex++;
    }
  }

  return [...types].filter(t => t.length > 0);
}

function parseRequirements(html: string): string {
  const section = extractSection(html, 'what-to-do') ||
                  extractSection(html, 'what-you-must-do') ||
                  extractSection(html, 'what-you-need-to-do');
  if (!section) return '';
  return stripHtml(section).slice(0, 2000);
}

function parseActionCode(title: string): { code: string; name: string } {
  // Titles follow pattern "CODE: Name" e.g. "CSAM3: Herbal leys"
  const match = title.match(/^([A-Z0-9]+):\s*(.+)$/);
  if (match) return { code: match[1], name: match[2].trim() };
  return { code: '', name: title };
}

function parseAction(searchResult: SearchResult, detail: ContentDetail): ParsedAction {
  const { code, name } = parseActionCode(detail.title);
  const body = detail.details.body || '';
  const metadata = detail.details.metadata || {};

  const payment = parsePaymentRate(body);
  const duration = parseDuration(body);
  const description = parseDescription(body);
  const landTypes = parseLandTypes(body, metadata);
  const requirements = parseRequirements(body);

  return {
    code,
    name,
    description,
    paymentRate: payment.rate,
    paymentUnit: payment.unit,
    paymentRateRaw: payment.raw,
    durationYears: duration,
    eligibleLandTypes: landTypes,
    requirements,
    link: searchResult.link,
  };
}

// ── Cross-Compliance Data ───────────────────────────────────────

interface CrossComplianceEntry {
  id: string;
  requirement: string;
  category: 'GAEC' | 'SMR';
  reference: string;
  description: string;
  applies_to: string;
}

const CROSS_COMPLIANCE: CrossComplianceEntry[] = [
  // GAEC standards
  {
    id: 'gaec-1',
    requirement: 'Establishment of buffer strips along water courses',
    category: 'GAEC',
    reference: 'GAEC 1',
    description: 'Maintain a buffer strip of at least 2 metres along watercourses. No cultivation, fertiliser, or pesticide application within the buffer strip.',
    applies_to: 'All land within 2m of a watercourse',
  },
  {
    id: 'gaec-2',
    requirement: 'Water abstraction compliance',
    category: 'GAEC',
    reference: 'GAEC 2',
    description: 'Comply with water abstraction licence conditions. Do not abstract water without a licence where one is required. Follow Environment Agency abstraction rules.',
    applies_to: 'All farmland where water is abstracted for irrigation or other purposes',
  },
  {
    id: 'gaec-3',
    requirement: 'Groundwater protection',
    category: 'GAEC',
    reference: 'GAEC 3',
    description: 'Protect groundwater against pollution and deterioration. Do not discharge pollutants (including fertilisers, pesticides, and fuel) into groundwater directly or indirectly.',
    applies_to: 'All farmland, with particular emphasis on areas above aquifers and groundwater source protection zones',
  },
  {
    id: 'gaec-4',
    requirement: 'Minimum soil cover',
    category: 'GAEC',
    reference: 'GAEC 4',
    description: 'Establish minimum soil cover to avoid bare soil in periods that are most sensitive to erosion. Maintain green cover or crop residues on land at risk of erosion, particularly over winter.',
    applies_to: 'Arable land and land at risk of soil erosion',
  },
  {
    id: 'gaec-5',
    requirement: 'Minimum tillage to reduce erosion',
    category: 'GAEC',
    reference: 'GAEC 5',
    description: 'Manage tillage to minimise the risk of soil degradation and erosion, taking into account slope, soil type, and climatic conditions. Avoid practices that increase erosion risk such as ploughing up and down slopes.',
    applies_to: 'All cultivated agricultural land',
  },
  {
    id: 'gaec-6',
    requirement: 'Maintain soil organic matter',
    category: 'GAEC',
    reference: 'GAEC 6',
    description: 'Maintain soil organic matter levels through appropriate practices including crop rotation. Do not burn arable stubble except under licence. Manage crop residues to maintain soil organic matter.',
    applies_to: 'All arable land',
  },
  {
    id: 'gaec-7',
    requirement: 'Landscape features retention',
    category: 'GAEC',
    reference: 'GAEC 7',
    description: 'Retain landscape features including hedgerows, ponds, ditches, trees (in groups, lines, or individual), field margins, terraces, and stone walls. Do not remove or destroy protected landscape features.',
    applies_to: 'All agricultural land with protected landscape features',
  },
  {
    id: 'gaec-8',
    requirement: 'Minimum share of arable land for non-productive areas',
    category: 'GAEC',
    reference: 'GAEC 8',
    description: 'Maintain a minimum of 4% of arable land as non-productive areas or features. This can include fallow land, landscape features, buffer strips, and hedgerows. Alternatively, dedicate 7% to non-productive areas including catch crops or nitrogen-fixing crops.',
    applies_to: 'All holdings with more than 10 hectares of arable land',
  },
  {
    id: 'gaec-9',
    requirement: 'Protection of environmentally sensitive permanent grassland',
    category: 'GAEC',
    reference: 'GAEC 9',
    description: 'Do not convert or plough environmentally sensitive permanent grassland designated within Natura 2000 sites (SACs and SPAs). Maintain the ecological character of these grasslands.',
    applies_to: 'Environmentally sensitive permanent grassland within or adjacent to Natura 2000 sites',
  },

  // SMR standards
  {
    id: 'smr-1',
    requirement: 'Water Framework Directive',
    category: 'SMR',
    reference: 'SMR 1',
    description: 'Comply with measures to protect water bodies from pollution. Follow the Nitrate Pollution Prevention Regulations and the Farming Rules for Water. Restrictions on timing, quantity, and storage of organic manure and nitrogen fertiliser in Nitrate Vulnerable Zones.',
    applies_to: 'All farmland, with additional rules in Nitrate Vulnerable Zones',
  },
  {
    id: 'smr-2',
    requirement: 'Nitrate Vulnerable Zones',
    category: 'SMR',
    reference: 'SMR 2',
    description: 'In designated Nitrate Vulnerable Zones, comply with restrictions on nitrogen application rates, closed periods for spreading, and storage requirements for organic manure. Keep records of nitrogen use.',
    applies_to: 'All land designated as Nitrate Vulnerable Zones (approximately 55% of England)',
  },
  {
    id: 'smr-3',
    requirement: 'Wild Birds Directive',
    category: 'SMR',
    reference: 'SMR 3',
    description: 'Protect wild bird species and their habitats. Do not deliberately kill or capture wild birds, destroy nests or eggs. Comply with Special Protection Area (SPA) management requirements.',
    applies_to: 'All agricultural land, with additional requirements near SPAs',
  },
  {
    id: 'smr-4',
    requirement: 'Food safety (contaminants)',
    category: 'SMR',
    reference: 'SMR 4',
    description: 'Ensure food produced on the farm meets food safety requirements regarding contaminants. Apply maximum residue levels for pesticides. Follow safe use of veterinary medicines. Keep records of treatments applied to crops and animals.',
    applies_to: 'All farms producing food for human consumption',
  },
  {
    id: 'smr-5',
    requirement: 'Hormone and thyrostatic substance restrictions',
    category: 'SMR',
    reference: 'SMR 5',
    description: 'Do not administer hormonal or thyrostatic substances to farm animals for growth promotion purposes. Only use such substances for therapeutic or zootechnical treatment under veterinary supervision.',
    applies_to: 'All livestock farms',
  },
  {
    id: 'smr-6',
    requirement: 'Plant protection products',
    category: 'SMR',
    reference: 'SMR 6',
    description: 'Only use plant protection products (pesticides) that are authorised. Store products securely. Follow label instructions for application rates and buffer zones. Hold required certificates for application (PA1/PA6). Keep spray records.',
    applies_to: 'All farms using plant protection products',
  },
  {
    id: 'smr-7',
    requirement: 'Animal identification (cattle)',
    category: 'SMR',
    reference: 'SMR 7',
    description: 'Tag all cattle with approved ear tags within 20 days of birth (36 hours for movements off holding). Maintain a herd register. Report births, deaths, and movements to BCMS within specified timeframes.',
    applies_to: 'All holdings with cattle',
  },
  {
    id: 'smr-8',
    requirement: 'Animal identification (sheep and goats)',
    category: 'SMR',
    reference: 'SMR 8',
    description: 'Tag all sheep and goats with approved identification before they leave the holding of birth. Maintain a flock/herd register. Report movements using the ARAMS system. Keep movement records for 3 years.',
    applies_to: 'All holdings with sheep or goats',
  },
  {
    id: 'smr-9',
    requirement: 'Animal identification (pigs)',
    category: 'SMR',
    reference: 'SMR 9',
    description: 'Identify all pigs with an approved ear tag, tattoo, or slap mark before they leave the holding. Maintain a herd register. Report movements through eAML2. Keep records of pig movements for 3 years.',
    applies_to: 'All holdings with pigs',
  },
  {
    id: 'smr-10',
    requirement: 'Animal disease prevention',
    category: 'SMR',
    reference: 'SMR 10',
    description: 'Report notifiable diseases immediately to APHA. Comply with disease control measures and movement restrictions when in force. Maintain biosecurity. Keep veterinary medicine records. Do not use prohibited substances.',
    applies_to: 'All livestock holdings',
  },
  {
    id: 'smr-11',
    requirement: 'Animal welfare (calves)',
    category: 'SMR',
    reference: 'SMR 11',
    description: 'Provide calves with adequate space, lighting, ventilation, and clean bedding. Calves must not be confined in individual pens after 8 weeks (unless veterinary advice). Provide sufficient colostrum within 6 hours of birth. Feed fibre from 2 weeks.',
    applies_to: 'All holdings rearing calves',
  },
  {
    id: 'smr-12',
    requirement: 'Animal welfare (pigs)',
    category: 'SMR',
    reference: 'SMR 12',
    description: 'Provide pigs with adequate space, enrichment materials, and access to feed and water. Sows must not be individually confined in stalls (gestation crates banned). Tail docking only as a last resort with veterinary evidence.',
    applies_to: 'All pig holdings',
  },
  {
    id: 'smr-13',
    requirement: 'Animal welfare (farm animals)',
    category: 'SMR',
    reference: 'SMR 13',
    description: 'Meet the welfare needs of all farm animals: adequate nutrition, suitable environment, protection from pain and disease, ability to express normal behaviour, and protection from fear and distress. Provide training for stockpersons.',
    applies_to: 'All livestock holdings',
  },
];

// ── Database Insertion ──────────────────────────────────────────

interface SchemeDefinition {
  id: string;
  name: string;
  scheme_type: string;
  authority: string;
  status: string;
  start_date: string;
  description: string;
  eligibility_summary: string;
  application_window: string;
  jurisdiction: string;
}

const SCHEME_SFI: SchemeDefinition = {
  id: 'sustainable-farming-incentive',
  name: 'Sustainable Farming Incentive',
  scheme_type: 'agri-environment',
  authority: 'DEFRA / RPA',
  status: 'open',
  start_date: '2023-01-01',
  description: 'Pays farmers for sustainable farming actions that support food production and benefit the environment. Part of the Environmental Land Management (ELM) scheme replacing the EU Common Agricultural Policy Basic Payment Scheme.',
  eligibility_summary: 'Must have at least 1 hectare of eligible land registered on the Rural Payments service. Must be the land manager of the land.',
  application_window: 'Rolling application -- apply any time via the Rural Payments service',
  jurisdiction: 'GB',
};

const SCHEME_CS: SchemeDefinition = {
  id: 'countryside-stewardship',
  name: 'Countryside Stewardship',
  scheme_type: 'agri-environment',
  authority: 'DEFRA / Natural England',
  status: 'open',
  start_date: '2015-01-01',
  description: 'Provides financial incentives for land managers to look after their environment by conserving and restoring wildlife habitats, managing flood risk, and maintaining woodland. The Higher Tier targets the most environmentally significant sites, commons, and woodland.',
  eligibility_summary: 'Must be the land manager of eligible land. Higher Tier requires a Natural England or Forestry Commission agreement. Land must meet specific environmental criteria.',
  application_window: 'Annual application window -- typically February to July',
  jurisdiction: 'GB',
};

function insertScheme(db: Database, scheme: SchemeDefinition): void {
  db.run(
    `INSERT OR REPLACE INTO schemes (id, name, scheme_type, authority, status, start_date, description, eligibility_summary, application_window, jurisdiction)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      scheme.id, scheme.name, scheme.scheme_type, scheme.authority, scheme.status,
      scheme.start_date, scheme.description, scheme.eligibility_summary,
      scheme.application_window, scheme.jurisdiction,
    ]
  );
}

function insertAction(db: Database, action: ParsedAction, schemeId: string): void {
  const optionId = action.code ? action.code.toLowerCase() : action.link.split('/').pop() || '';

  db.run(
    `INSERT OR REPLACE INTO scheme_options
       (id, scheme_id, code, name, description, payment_rate, payment_unit, eligible_land_types, requirements, duration_years, stacking_rules, jurisdiction)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      optionId,
      schemeId,
      action.code || null,
      action.name,
      action.description || null,
      action.paymentRate,
      action.paymentUnit || action.paymentRateRaw || null,
      action.eligibleLandTypes.length > 0 ? JSON.stringify(action.eligibleLandTypes) : null,
      action.requirements || null,
      action.durationYears,
      null, // stacking_rules -- not consistently available in the API
      'GB',
    ]
  );
}

function insertCrossCompliance(db: Database): void {
  for (const cc of CROSS_COMPLIANCE) {
    db.run(
      `INSERT OR REPLACE INTO cross_compliance (id, requirement, category, reference, description, applies_to, jurisdiction)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [cc.id, cc.requirement, cc.category, cc.reference, cc.description, cc.applies_to, 'GB']
    );
  }
}

function buildSearchIndex(
  db: Database,
  sfiActions: ParsedAction[],
  csActions: ParsedAction[]
): number {
  // Clear existing FTS entries
  db.run('DELETE FROM search_index');

  let count = 0;
  const allActions = [...sfiActions, ...csActions];

  // Index each action (SFI + CS)
  for (const a of allActions) {
    const title = a.code ? `${a.code}: ${a.name}` : a.name;
    const body = [
      a.name,
      a.description,
      a.paymentRateRaw ? `Payment: ${a.paymentRateRaw}` : '',
      a.eligibleLandTypes.length > 0 ? `Eligible land: ${a.eligibleLandTypes.join(', ')}` : '',
      a.requirements ? `Requirements: ${a.requirements.slice(0, 500)}` : '',
    ].filter(Boolean).join('. ');

    db.run(
      'INSERT INTO search_index (title, body, scheme_type, jurisdiction) VALUES (?, ?, ?, ?)',
      [title, body, 'agri-environment', 'GB']
    );
    count++;
  }

  // Index SFI scheme overview
  db.run(
    'INSERT INTO search_index (title, body, scheme_type, jurisdiction) VALUES (?, ?, ?, ?)',
    [
      'Sustainable Farming Incentive (SFI)',
      'DEFRA SFI scheme paying farmers for sustainable farming actions. Rolling applications year-round. ' +
      'Actions cover soil health, hedgerows, nutrient management, pest management, wildlife habitats, ' +
      'water quality, moorland management, agroforestry, organic farming, and precision farming. ' +
      `${sfiActions.length} actions available.`,
      'agri-environment',
      'GB',
    ]
  );
  count++;

  // Index SFI application guidance
  db.run(
    'INSERT INTO search_index (title, body, scheme_type, jurisdiction) VALUES (?, ?, ?, ?)',
    [
      'How to Apply for SFI',
      'Apply for the Sustainable Farming Incentive through the Rural Payments service. ' +
      'Rolling applications accepted year-round. You need a Customer Reference Number (CRN) ' +
      'and at least 1 hectare of eligible land registered with RPA. Agreement lasts 3 years. ' +
      'Can apply for multiple actions on the same or different land parcels.',
      'agri-environment',
      'GB',
    ]
  );
  count++;

  // Index CS Higher Tier scheme overview
  db.run(
    'INSERT INTO search_index (title, body, scheme_type, jurisdiction) VALUES (?, ?, ?, ?)',
    [
      'Countryside Stewardship Higher Tier',
      'DEFRA / Natural England Countryside Stewardship Higher Tier scheme providing financial incentives ' +
      'for land managers to conserve and restore wildlife habitats, manage flood risk, and maintain woodland. ' +
      'Targets the most environmentally significant sites, commons, and woodland. ' +
      'Annual application window, typically February to July. Requires Natural England agreement. ' +
      `${csActions.length} actions available.`,
      'agri-environment',
      'GB',
    ]
  );
  count++;

  // Index CS application guidance
  db.run(
    'INSERT INTO search_index (title, body, scheme_type, jurisdiction) VALUES (?, ?, ?, ?)',
    [
      'How to Apply for Countryside Stewardship Higher Tier',
      'Apply for Countryside Stewardship Higher Tier through Natural England. ' +
      'Annual application window, typically February to July. Requires a Natural England or Forestry Commission agreement. ' +
      'Land must meet specific environmental criteria. Agreement duration is typically 5 or 10 years. ' +
      'Can include capital items alongside revenue options.',
      'agri-environment',
      'GB',
    ]
  );
  count++;

  // Index cross-compliance requirements
  for (const cc of CROSS_COMPLIANCE) {
    db.run(
      'INSERT INTO search_index (title, body, scheme_type, jurisdiction) VALUES (?, ?, ?, ?)',
      [
        `${cc.reference}: ${cc.requirement}`,
        `${cc.category} cross-compliance requirement. ${cc.description} Applies to: ${cc.applies_to}.`,
        'cross-compliance',
        'GB',
      ]
    );
    count++;
  }

  return count;
}

function updateMetadata(db: Database, actionCount: number, ftsCount: number): void {
  const now = new Date().toISOString().split('T')[0];
  db.run("INSERT OR REPLACE INTO db_metadata (key, value) VALUES ('last_ingest', ?)", [now]);
  db.run("INSERT OR REPLACE INTO db_metadata (key, value) VALUES ('build_date', ?)", [now]);
  db.run("INSERT OR REPLACE INTO db_metadata (key, value) VALUES ('scheme_option_count', ?)", [String(actionCount)]);
  db.run("INSERT OR REPLACE INTO db_metadata (key, value) VALUES ('cross_compliance_count', ?)", [String(CROSS_COMPLIANCE.length)]);
  db.run("INSERT OR REPLACE INTO db_metadata (key, value) VALUES ('fts_entry_count', ?)", [String(ftsCount)]);
  db.run("INSERT OR REPLACE INTO db_metadata (key, value) VALUES ('source', 'GOV.UK Content API')", []);
}

function writeCoverage(
  sfiActions: ParsedAction[],
  csActions: ParsedAction[],
  ftsCount: number
): void {
  const now = new Date().toISOString().split('T')[0];
  const allActions = [...sfiActions, ...csActions];
  const withRate = allActions.filter(a => a.paymentRate !== null).length;
  const withDuration = allActions.filter(a => a.durationYears !== null).length;

  const coverage = {
    mcp_name: 'UK Farm Subsidies MCP',
    jurisdiction: 'GB',
    build_date: now,
    source: 'GOV.UK Content API',
    schemes: 2,
    scheme_options: allActions.length,
    scheme_options_sfi: sfiActions.length,
    scheme_options_cs_higher_tier: csActions.length,
    scheme_options_with_payment_rate: withRate,
    scheme_options_with_duration: withDuration,
    cross_compliance_requirements: CROSS_COMPLIANCE.length,
    fts_entries: ftsCount,
    source_hash: createHash('sha256')
      .update(JSON.stringify(allActions.map(a => a.code).sort()))
      .digest('hex')
      .slice(0, 16),
  };

  writeFileSync('data/coverage.json', JSON.stringify(coverage, null, 2) + '\n');
  console.log('Wrote data/coverage.json');
}

// ── Main ────────────────────────────────────────────────────────

async function fetchAndParseActions(
  grantType: string,
  label: string
): Promise<{ actions: ParsedAction[]; failed: number }> {
  const searchResults = await fetchAllSearchResults(grantType, label);

  const actions: ParsedAction[] = [];
  let fetched = 0;
  let failed = 0;

  for (const sr of searchResults) {
    const detail = await fetchContentDetail(sr.link);
    if (detail) {
      const action = parseAction(sr, detail);
      actions.push(action);
      fetched++;
      if (fetched % 10 === 0) {
        console.log(`  Fetched ${fetched}/${searchResults.length} ${label}...`);
      }
    } else {
      failed++;
    }
    await sleep(API_DELAY_MS);
  }

  console.log(`  Fetched ${fetched} ${label} details (${failed} failed).`);
  return { actions, failed };
}

async function main(): Promise<void> {
  mkdirSync('data', { recursive: true });
  mkdirSync(CACHE_DIR, { recursive: true });

  console.log('UK Farm Subsidies MCP -- Data Ingestion');
  console.log('====================================\n');

  // Step 1: Fetch all SFI actions from search API
  console.log('Step 1: Fetching SFI action list from GOV.UK...');
  const sfiSearch = await fetchAndParseActions('sustainable-farming-incentive', 'SFI');
  const sfiActions = sfiSearch.actions;

  // Step 2: Fetch all CS Higher Tier actions from search API
  console.log('\nStep 2: Fetching CS Higher Tier action list from GOV.UK...');
  const csSearch = await fetchAndParseActions('countryside-stewardship-higher-tier', 'CS Higher Tier');
  const csActions = csSearch.actions;

  const totalFailed = sfiSearch.failed + csSearch.failed;
  const allActions = [...sfiActions, ...csActions];

  if (FETCH_ONLY) {
    console.log('\n--fetch-only: skipping database writes.');
    console.log(`Cached ${allActions.length} action details in ${CACHE_DIR}/`);
    return;
  }

  // Step 3: Write to database
  console.log('\nStep 3: Writing to database...');
  const db = createDatabase('data/database.db');

  try {
    // Use a transaction for atomicity
    db.instance.exec('BEGIN TRANSACTION');

    // Clear existing data
    db.run('DELETE FROM scheme_options');
    db.run('DELETE FROM schemes');
    db.run('DELETE FROM cross_compliance');

    // Insert SFI scheme
    insertScheme(db, SCHEME_SFI);
    console.log('  Inserted SFI scheme.');

    // Insert CS Higher Tier scheme
    insertScheme(db, SCHEME_CS);
    console.log('  Inserted CS Higher Tier scheme.');

    // Insert SFI actions
    for (const action of sfiActions) {
      insertAction(db, action, SCHEME_SFI.id);
    }
    console.log(`  Inserted ${sfiActions.length} SFI scheme options.`);

    // Insert CS Higher Tier actions
    for (const action of csActions) {
      insertAction(db, action, SCHEME_CS.id);
    }
    console.log(`  Inserted ${csActions.length} CS Higher Tier scheme options.`);

    // Insert cross-compliance
    insertCrossCompliance(db);
    console.log(`  Inserted ${CROSS_COMPLIANCE.length} cross-compliance requirements.`);

    // Build FTS5 index
    console.log('\nStep 4: Building FTS5 search index...');
    const ftsCount = buildSearchIndex(db, sfiActions, csActions);
    console.log(`  Created ${ftsCount} FTS5 entries.`);

    // Update metadata
    updateMetadata(db, allActions.length, ftsCount);

    db.instance.exec('COMMIT');

    // Write coverage.json
    console.log('\nStep 5: Writing coverage data...');
    writeCoverage(sfiActions, csActions, ftsCount);

    // Summary
    console.log('\nIngestion complete.');
    console.log('-------------------');
    console.log(`  Schemes:              2`);
    console.log(`  Scheme options (SFI): ${sfiActions.length}`);
    console.log(`  Scheme options (CS):  ${csActions.length}`);
    console.log(`  Scheme options total: ${allActions.length}`);
    console.log(`  Cross-compliance:     ${CROSS_COMPLIANCE.length}`);
    console.log(`  FTS5 entries:         ${ftsCount}`);
    console.log(`  Actions with rate:    ${allActions.filter(a => a.paymentRate !== null).length}`);
    console.log(`  Actions with duration:${allActions.filter(a => a.durationYears !== null).length}`);
    if (totalFailed > 0) {
      console.log(`  Fetch failures:       ${totalFailed}`);
    }

    // Log any actions without parsed payment rates
    const missingRates = allActions.filter(a => a.paymentRate === null);
    if (missingRates.length > 0) {
      console.log(`\n  WARN: ${missingRates.length} actions missing payment rate:`);
      for (const a of missingRates) {
        console.log(`    - ${a.code || '(no code)'}: ${a.name}`);
      }
    }
  } catch (err) {
    db.instance.exec('ROLLBACK');
    throw err;
  } finally {
    db.close();
  }
}

main().catch((err) => {
  console.error(`\nFATAL: ${err instanceof Error ? err.message : String(err)}`);
  process.exit(1);
});
