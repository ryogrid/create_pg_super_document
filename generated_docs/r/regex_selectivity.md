# regex_selectivity

## Location
src/backend/utils/adt/like_support.c: 1455 - 1500

## Overview
Main entry point for estimating the selectivity of regular expression patterns, handling trailing anchors and fixed prefix adjustments.

## Definition
```c
static Selectivity regex_selectivity(const char *patt, int pattlen, bool case_insensitive,
                                     int fixed_prefix_len)
```

## Detailed Description
This function serves as the main interface for regular expression selectivity estimation. It coordinates the overall selectivity calculation by:

**Trailing Anchor Handling:**
- Detects unescaped trailing `$` anchors that force end-of-string matching
- For patterns without trailing `$`, applies FULL_WILDCARD_SEL factor to account for the implicit "match anywhere in string" behavior
- Properly handles escaped `\$` sequences that represent literal dollar signs

**Core Selectivity Calculation:**
- Delegates the main pattern analysis to `regex_selectivity_sub()` for the actual regex parsing
- Strips trailing `$` before analysis since it affects matching behavior but not character-level selectivity

**Fixed Prefix Compensation:**
- Adjusts selectivity to account for fixed prefixes that have already been factored into prefix-based optimizations
- Calculates prefix selectivity using `pow(FIXED_CHAR_SEL, fixed_prefix_len)` and divides it out
- Includes safety checks to prevent division by zero from numerical underflow

**Result Validation:**
- Uses CLAMP_PROBABILITY to ensure the final result stays within valid probability bounds [0,1]

## Parameters / Member Variables
- `patt`: Pointer to the regular expression pattern string to analyze
- `pattlen`: Length of the pattern string in characters
- `case_insensitive`: Boolean flag indicating case-insensitive matching mode
- `fixed_prefix_len`: Length of any fixed prefix that has been extracted and will be handled separately (used to avoid double-counting)

## Dependencies
- Functions called/Symbols referenced:
  - regex_selectivity_sub (performs the detailed regex pattern analysis)
  - FULL_WILDCARD_SEL (selectivity factor for implicit trailing wildcard)
  - FIXED_CHAR_SEL (base selectivity for literal characters, used in prefix calculations)
  - pow (mathematical power function for prefix selectivity calculation)
  - CLAMP_PROBABILITY (macro to ensure result stays in valid range)
- Called from:
  - regex_fixed_prefix (when calculating selectivity for remaining pattern after prefix extraction)

## Notes and Other Information
- This is a static function within like_support.c, not exposed in the public API
- The function handles the semantic difference between anchored (`$`) and unanchored regex patterns
- Fixed prefix length compensation prevents double-counting selectivity when prefix optimization is used
- The `pow()` calculation for prefix selectivity can underflow to zero for very long prefixes, which is handled gracefully
- The trailing `$` detection includes escape sequence handling to distinguish `\$` (literal) from `$` (anchor)
- CLAMP_PROBABILITY ensures robust behavior even with numerical edge cases or calculation errors
- This function bridges the gap between high-level pattern matching needs and the low-level recursive pattern analysis