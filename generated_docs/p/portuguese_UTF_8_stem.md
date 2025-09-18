# portuguese_UTF_8_stem

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_portuguese.c:884-963](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_portuguese.c#L884-L963)

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
  - [r_prelude](../r/r_prelude.md)
  - [r_mark_regions](../r/r_mark_regions.md)  
  - [r_standard_suffix](../r/r_standard_suffix.md)
  - [r_verb_suffix](../r/r_verb_suffix.md)
  - [r_RV](../r/r_RV.md)
  - [slice_del](../s/slice_del.md)
  - [r_residual_suffix](../r/r_residual_suffix.md)
  - [r_residual_form](../r/r_residual_form.md)
  - [r_postlude](../r/r_postlude.md)
- Called from (representative examples):
  - No direct references found (likely called via function pointers or external interfaces)

## Notes and Other Information
The function implements a sophisticated backtracking mechanism where if standard suffix removal fails, it attempts verb suffix removal, and if that fails, it tries residual suffix processing. It includes a special case for removing "i" preceded by "c" in the RV region. The function always returns 1 on successful completion, with negative values indicating errors from the underlying processing functions. The algorithm maintains proper cursor positioning throughout the multi-phase processing pipeline.