# portuguese_ISO_8859_1_stem

## Location
src/backend/snowball/libstemmer/stem_ISO_8859_1_portuguese.c: 878 - 957

## Overview
The main stemming function that implements the complete Portuguese Snowball stemming algorithm for ISO-8859-1 encoded text, reducing Portuguese words to their stem form through a multi-stage process.

## Definition
```c
extern int portuguese_ISO_8859_1_stem(struct SN_env * z)
```

## Detailed Description
This function orchestrates the complete Portuguese stemming process using the Snowball algorithm. It processes Portuguese words encoded in ISO-8859-1 character set through multiple sequential stages:

1. **Preprocessing**: Applies character normalization and initial transformations
2. **Region Marking**: Identifies morphological regions (R1, R2, RV) within the word
3. **Suffix Processing**: Attempts multiple suffix removal strategies in order of priority:
   - Standard suffix removal (highest priority)
   - Verb suffix removal (if standard fails)
   - Residual suffix removal (if verb fails)
4. **Special Case Handling**: Removes specific patterns like 'ci' endings in RV region
5. **Residual Form Processing**: Handles remaining word forms through pattern matching
6. **Postprocessing**: Performs final character transformations and cleanup

The function uses a state machine approach with backtracking, where different suffix removal strategies are tried in sequence until one succeeds or all fail.

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing the stemming environment with the word to be processed, cursor positions, and region boundaries

## Dependencies
- Functions called/Symbols referenced:
  - r_prelude (line 880): Character normalization and preprocessing
  - r_mark_regions (line 886): Identifies R1, R2, and RV regions
  - r_standard_suffix (line 895): Main suffix removal logic
  - r_verb_suffix (line 902): Verb-specific suffix handling
  - r_RV (line 919): Boundary checking for RV region
  - r_residual_suffix (line 933): Handles remaining suffix patterns
  - r_residual_form (line 943): Processes residual word forms
  - r_postlude (line 950): Final character transformations
  - slice_del: String deletion utility (called inline)
- Called from:
  - External stemming interface (not referenced within this codebase)

## Notes and Other Information
- Returns 1 on successful completion, negative values indicate errors
- Uses complex control flow with labeled gotos for backtracking between different suffix removal strategies
- Maintains multiple cursor position markers (m2-m8) for backtracking capability
- Processes text from right-to-left during suffix removal phases
- Specific to ISO-8859-1 character encoding for Portuguese text
- Part of the libstemmer library implementation for PostgreSQL's text search functionality