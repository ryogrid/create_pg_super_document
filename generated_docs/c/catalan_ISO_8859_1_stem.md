# catalan_ISO_8859_1_stem

## Location
src/backend/snowball/libstemmer/stem_ISO_8859_1_catalan.c: 1396 - 1442

## Overview
The `catalan_ISO_8859_1_stem` function is the main entry point for the Catalan language stemming algorithm, coordinating the complete stemming process through multiple processing stages for ISO-8859-1 encoded text.

## Definition
```c
extern int catalan_ISO_8859_1_stem(struct SN_env * z)
```

## Detailed Description
This function implements the complete Catalan stemming algorithm pipeline, processing words through six distinct stages:

1. **Region Marking**: Calls `r_mark_regions` to identify R1 and R2 morphological regions within the word
2. **Pronoun Processing**: Uses `r_attached_pronoun` to identify and remove attached pronouns 
3. **Suffix Processing**: Attempts suffix removal in order of priority:
   - First tries `r_standard_suffix` for general suffix patterns
   - Falls back to `r_verb_suffix` if no standard suffix matches
4. **Residual Processing**: Applies `r_residual_suffix` to handle remaining morphological elements
5. **Text Cleaning**: Performs final `r_cleaning` operations for character normalization

The algorithm uses a sophisticated backtracking mechanism with saved cursor positions (m1-m4, c5) to ensure proper word processing and enable fallback operations between different suffix types. The function processes the word from right to left (backwards) for suffix identification while maintaining proper cursor management.

## Parameters / Member Variables
- `z`: Pointer to the SN_env structure containing:
  - Word buffer and cursor positions (c, l, lb)
  - Region boundaries for morphological analysis
  - Processing state and temporary markers

## Dependencies
- Functions called/Symbols referenced:
  - r_mark_regions: Identifies morphological regions (R1, R2)
  - r_attached_pronoun: Processes attached pronouns
  - r_standard_suffix: Handles general suffix patterns (200 patterns)
  - r_verb_suffix: Processes verb suffixes (283 patterns) 
  - r_residual_suffix: Cleans up remaining suffixes (22 patterns)
  - r_cleaning: Performs character normalization and cleanup
- Called from (representative examples):
  - External stemming interface (likely from PostgreSQL text search components)
  - Language-specific stemming drivers

## Notes and Other Information
- This function serves as the public interface (extern) for Catalan stemming in the ISO-8859-1 character encoding
- The algorithm follows the Snowball stemming methodology with multi-stage processing
- Uses cursor position management with backtracking to handle different processing paths
- The suffix processing uses priority ordering: standard suffixes are processed before verb suffixes
- Return value of 1 indicates successful stemming completion
- Handles both suffix removal and character cleaning operations
- The function is encoding-specific (ISO-8859-1), with a corresponding UTF-8 variant available
- Part of PostgreSQL's full-text search capabilities for Catalan language support