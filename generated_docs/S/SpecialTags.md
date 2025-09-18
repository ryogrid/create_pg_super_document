# SpecialTags

## Location
src/backend/tsearch/wparser_def.c: 564 - 587

## Overview
A static function that handles special HTML tags (script and style) by setting the parser's ignore flag to control text processing within these tags.

## Definition
```c
static void SpecialTags(TParser *prs)
```

## Detailed Description
This function processes HTML tags that require special handling during text parsing, specifically `<script>`, `</script>`, `<style>`, and `</style>` tags. It manages the parser's ignore flag to control whether content within these tags should be processed for text search purposes.

The function uses a switch statement based on the token length to efficiently identify and handle these special tags:
- When encountering opening tags (`<script>` or `<style>`), it sets the ignore flag to true, causing the parser to skip content within these tags
- When encountering closing tags (`</script>` or `</style>`), it sets the ignore flag to false, resuming normal text processing

This is crucial for HTML document parsing where script and style content should typically be excluded from full-text search indexing.

## Parameters / Member Variables
- `prs`: Pointer to TParser structure containing the current parsing state, token information, and control flags

## Dependencies
- Functions called/Symbols referenced:
  - TParser (structure type)
  - pg_strncasecmp (for case-insensitive string comparison)
- Called from (representative examples):
  - p_isspecial (at src/backend/tsearch/wparser_def.c:1227)
  - p_isspecial (at src/backend/tsearch/wparser_def.c:1228)
  - p_isspecial (at src/backend/tsearch/wparser_def.c:1245)
  - p_isspecial (at src/backend/tsearch/wparser_def.c:1261)

## Notes and Other Information
- Uses case-insensitive comparison via pg_strncasecmp to handle various HTML tag capitalizations
- Optimized with length-based switching to avoid unnecessary string comparisons
- The ignore flag mechanism allows the parser to skip content that should not be included in text search operations
- Part of PostgreSQL's HTML-aware text search functionality
- Handles both opening and closing variants of script and style tags
- Length checks: 6 chars for `<style`, 7 chars for `<script` and `</style`, 8 chars for `</script`