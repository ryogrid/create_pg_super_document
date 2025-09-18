# pg_stats_ext_mcvlist_items

## Location
src/backend/statistics/mcv.c: 1338 - 1471

## Overview
A Set-Returning Function (SRF) that exposes detailed information about individual items in a Most Common Values (MCV) statistics list as SQL-accessible tuples for administrative and analytical purposes.

## Definition


## Detailed Description
This function provides SQL access to MCV list contents by deserializing the binary statistics data and returning each MCV item as a tuple. The function operates as a set-returning function, yielding one tuple per MCV item containing:

- **Item ID**: Sequential index (0 to nitems-1)  
- **Values**: Text array of the actual values for each dimension
- **Nulls**: Boolean array indicating null status for each dimension
- **Frequency**: Observed frequency of this value combination
- **Base frequency**: Expected frequency under independence assumption

The function handles the complete SRF lifecycle, including initialization on first call, per-call tuple generation, and cleanup on completion. It converts internal Datum values to their string representations using appropriate output functions for each data type.

## Parameters / Member Variables
- Function takes a single parameter via : The serialized MCV list data as bytea

## Dependencies
- Functions called/Symbols referenced:
  -  - Deserializes input bytea to MCVList
  - // - SRF management macros
  - / - Tuple descriptor setup
  - / - Array construction utilities
  - / - Type output conversion
  - / - Tuple creation
  - / - SRF return macros

- Called from (representative examples):
  - Exposed as SQL function for querying MCV list contents
  - Used by database administrators and query analysis tools

## Notes and Other Information
- Returns composite tuples with 5 columns: item_number, values[], nulls[], frequency, base_frequency
- Handles multi-dimensional statistics by building arrays for values and null flags
- Uses proper memory context management for multi-call function persistence  
- Converts internal Datum values to text representations using type-specific output functions
- Essential for introspecting extended statistics data and understanding query planner decisions
- The function will return no rows if the input statistics contains no MCV data