# portuguese_UTF_8_stem

## Location
src/backend/snowball/libstemmer/stem_UTF_8_portuguese.c: 884 - 963

## Overview
The portuguese_UTF_8_stem function is the main entry point for Portuguese word stemming using the Snowball algorithm, performing a complete stemming pipeline that reduces Portuguese words to their root forms.

## Definition
```c
extern int portuguese_UTF_8_stem(struct SN_env * z)
```

## Detailed Description
This function implements the complete Portuguese stemming algorithm by orchestrating a series of morphological processing steps. The function follows the standard Snowball stemming pipeline: preprocessing, region marking, suffix removal, residual processing, and post-processing. It processes words from right-to-left (suffix removal) and includes special handling for Portuguese-specific morphological patterns including verb suffixes, standard suffixes, and residual forms. The function uses backtracking mechanisms to try different suffix removal strategies when the primary approach fails.

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure containing the word to be stemmed, cursor positions, and processing state

## Dependencies
- Functions called/Symbols referenced:
  - r_prelude
  - r_mark_regions  
  - r_standard_suffix
  - r_verb_suffix
  - r_RV
  - slice_del
  - r_residual_suffix
  - r_residual_form
  - r_postlude
- Called from (representative examples):
  - No direct references found (likely called via function pointers or external interfaces)

## Notes and Other Information
The function implements a sophisticated backtracking mechanism where if standard suffix removal fails, it attempts verb suffix removal, and if that fails, it tries residual suffix processing. It includes a special case for removing "i" preceded by "c" in the RV region. The function always returns 1 on successful completion, with negative values indicating errors from the underlying processing functions. The algorithm maintains proper cursor positioning throughout the multi-phase processing pipeline.