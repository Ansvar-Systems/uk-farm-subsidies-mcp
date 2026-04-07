/**
 * Citation metadata for the deterministic citation pipeline.
 *
 * Provides structured identifiers (canonical_ref, display_text, aliases)
 * that the platform's entity linker uses to match references in agent
 * responses to MCP tool results — without relying on LLM formatting.
 *
 * This is the UNIVERSAL template — works for all MCP types (law, sector,
 * agriculture, domain). Each MCP adapts the builder call to its own
 * field names.
 *
 * See: docs/guides/law-mcp-golden-standard.md Section 4.9c
 */

export interface CitationMetadata {
  canonical_ref: string;
  display_text: string;
  aliases?: string[];
  source_url?: string;
  lookup: {
    tool: string;
    args: Record<string, string>;
  };
}

/**
 * Build citation metadata for any retrieval tool response.
 *
 * @param canonicalRef  Primary reference the entity linker matches against
 * @param displayText   How the reference appears in prose
 * @param toolName      The MCP tool name (e.g., "get_provision", "get_article")
 * @param toolArgs      The tool arguments for verification lookup
 * @param sourceUrl     Official portal URL (optional)
 * @param aliases       Alternative names the LLM might use (optional)
 */
export function buildCitation(
  canonicalRef: string,
  displayText: string,
  toolName: string,
  toolArgs: Record<string, string>,
  sourceUrl?: string | null,
  aliases?: string[],
): CitationMetadata {
  return {
    canonical_ref: canonicalRef,
    display_text: displayText,
    ...(aliases && aliases.length > 0 && { aliases }),
    ...(sourceUrl && { source_url: sourceUrl }),
    lookup: {
      tool: toolName,
      args: toolArgs,
    },
  };
}
