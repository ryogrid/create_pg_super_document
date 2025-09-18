# r_izenak

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_basque.c:1046-1116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_basque.c#L1046-L1116)

## Overview
A Basque-specific stemming function that handles noun suffix processing ("izenak" means "nouns" in Basque), implementing rule-based suffix removal and transformation for noun forms during text normalization.

## Definition
static int r_izenak(struct SN_env * z)

## Detailed Description
The r_izenak function processes Basque noun endings during stemming by matching against a predefined list of 295 noun suffixes (a_1 array). It operates by positioning cursors at word boundaries, identifying matching suffix patterns, and applying appropriate transformations based on morphological rules. The function uses a switch statement with 10 different cases to handle various types of noun suffix processing, including deletion in different regions (RV, R1, R2) and replacement with specific strings. This is part of the Snowball stemming algorithm implementation for Basque language support in PostgreSQL's full-text search functionality.

## Parameters / Member Variables
- : Pointer to SN_env structure containing the stemming environment with word data, cursors, and boundaries

## Dependencies
- Functions called/Symbols referenced:
  - [r_RV](r_RV.md) (region boundary test)
  - [r_R1](r_R1.md) (region boundary test)
  - [r_R2](r_R2.md) (region boundary test)
  - [find_among_b](../f/find_among_b.md) (suffix matching)
  - [slice_del](../s/slice_del.md) (suffix deletion)
  - [slice_from_s](../s/slice_from_s.md) (suffix replacement)
- Called from (representative examples):
  - [basque_ISO_8859_1_stem](../b/basque_ISO_8859_1_stem.md)
  - [basque_UTF_8_stem](../b/basque_UTF_8_stem.md)

## Notes and Other Information
This function is part of the Basque stemming algorithm and specifically handles noun morphology. It processes 295 different noun suffixes and applies context-sensitive transformations based on region boundaries (R1, R2, RV). The function returns 1 on successful processing and 0 if no matching suffix is found. Some cases perform simple deletion while others replace suffixes with specific strings (s_3 through s_9).