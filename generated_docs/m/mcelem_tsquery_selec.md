# mcelem_tsquery_selec

## Location
src/backend/tsearch/ts_selfuncs.c: 207 - 277

## Overview
Computes TSQuery selectivity using most-common-elements (MCELEM) statistics by building a lookup structure and delegating to query analysis functions.

## Definition


## Detailed Description
 processes PostgreSQL's most-common-elements statistics to estimate the selectivity of a TSQuery against a tsvector column. This function serves as a bridge between the raw statistical data stored in pg_statistic and the query analysis logic.

The function performs several key operations:
1. Validates that the statistics structure is consistent (nnumbers should equal nmcelem + 2)
2. Transposes the parallel arrays of elements and frequencies into a single searchable structure
3. Extracts the minimum frequency threshold from the statistics
4. Delegates the actual selectivity computation to 

The statistics format expects the last two numbers to contain minimum and maximum frequencies, which are used by the query evaluation logic to handle terms not present in the most-common-elements list.

## Parameters / Member Variables
- : TSQuery structure containing the search query to evaluate
- : Array of Datum values representing the most common lexemes in the column
- : Number of elements in the mcelem array
- : Array of float4 values containing frequency information for each element
- : Number of elements in the numbers array (should be nmcelem + 2)

## Dependencies
- Functions called/Symbols referenced:
  - tsquery_opr_selec_no_stats: Fallback when statistics format is invalid
  - palloc: Memory allocation for lookup structure
  - VARATT_IS_COMPRESSED/VARATT_IS_EXTERNAL: Validation macros for variable-length data
  - DatumGetPointer: Converts Datum to pointer for text data
  - tsquery_opr_selec: Core selectivity computation function
  - GETQUERY/GETOPERAND: Macros to extract query components from TSQuery
  - pfree: Memory deallocation
- Data structures used:
  - TextFreq: Structure pairing text elements with their frequencies
  - TSQuery: Text search query representation
- Called from (representative examples):
  - tsquerysel: Main selectivity estimation function

## Notes and Other Information
- Expects statistics in a specific format with exactly nmcelem + 2 numbers
- The last two numbers contain minimum and maximum frequencies from compute_tsvector_stats()
- Uses Assert() to validate that text data is not compressed or stored externally
- Creates a temporary lookup structure that gets freed after use
- The function handles the data transformation needed to use bsearch() for efficient lookups
- Part of PostgreSQL's statistics-based selectivity estimation for text search
- Returns to fallback estimation if the statistics format is unexpected