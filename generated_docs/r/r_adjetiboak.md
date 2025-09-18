# r_adjetiboak

## Location
src/backend/snowball/libstemmer/stem_UTF_8_basque.c: 1117 - 1141

## Overview
A Basque-specific stemming function that handles adjective suffix processing ("adjetiboak" means "adjectives" in Basque), implementing rule-based suffix removal and transformation for adjective forms during text normalization.

## Definition
static int r_adjetiboak(struct SN_env * z)

## Detailed Description
The r_adjetiboak function processes Basque adjective endings during stemming by matching against a predefined set of 19 adjective suffix patterns (a_2 array). It operates by positioning cursors at word boundaries, identifying matching suffix patterns, and applying appropriate transformations based on morphological rules. The function uses a simple switch statement with 2 cases: one for suffix deletion within the RV region and another for suffix replacement with a specific string (s_10). This is part of the Snowball stemming algorithm implementation for Basque language support in PostgreSQL's full-text search functionality.

## Parameters / Member Variables
- : Pointer to SN_env structure containing the stemming environment with word data, cursors, and boundaries

## Dependencies
- Functions called/Symbols referenced:
  - r_RV (region boundary test)
  - find_among_b (suffix matching)
  - slice_del (suffix deletion)
  - slice_from_s (suffix replacement)
- Called from (representative examples):
  - basque_ISO_8859_1_stem
  - basque_UTF_8_stem

## Notes and Other Information
This function is part of the Basque stemming algorithm and specifically handles adjective morphology. It processes 19 different adjective suffixes with simpler logic compared to noun processing (r_izenak). The function returns 1 on successful processing and 0 if no matching suffix is found. The relatively small number of patterns (19 vs 295 for nouns) reflects the simpler morphological structure of Basque adjectives compared to nouns.