# tsquery_opr_selec

## Location
src/backend/tsearch/ts_selfuncs.c: 278 - 433

## Overview
Recursively traverses TSQuery trees to compute selectivity estimates using statistics-based analysis for lexemes and probability theory for logical operators.

## Definition


## Detailed Description
 is the core recursive function that analyzes TSQuery expression trees to estimate selectivity. It implements a sophisticated algorithm that handles different types of query nodes:

**For lexeme nodes (QI_VAL):**
- **Exact matches**: Uses binary search in the MCELEM statistics to find precise frequencies, or falls back to estimated frequencies for uncommon terms
- **Prefix matches**: Scans through all MCELEMs to find matching prefixes and extrapolates to the entire lexeme population

**For operator nodes:**
- **AND/PHRASE**: Multiplies selectivities (assumes independence)
- **OR**: Uses inclusion-exclusion principle: 
- **NOT**: Complements selectivity: 

The function gracefully handles cases with insufficient or missing statistics by falling back to default estimates. It includes stack depth checking for protection against deeply nested queries.

## Parameters / Member Variables
- : Current QueryItem in the TSQuery tree being processed
- : String buffer containing all lexeme text for the query
- : Array of TextFreq structures containing most-common-elements statistics
- : Number of elements in the lookup array
- : Minimum frequency from statistics, used for estimation bounds

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth: Prevents stack overflow from deep recursion
  - bsearch: Binary search for exact lexeme matches in statistics
  - [compare_lexeme_textfreq](../c/compare_lexeme_textfreq.md): Comparison function for binary search
  - strncmp: String comparison for prefix matching
  - CLAMP_PROBABILITY: Ensures selectivity stays within [0,1] bounds
  - elog: Error logging for unrecognized operators
- Data structures used:
  - QueryItem: TSQuery tree node structure
  - QueryOperand: Lexeme node with distance, length, and prefix flag
  - [TextFreq](../T/TextFreq.md): Pairing of lexeme text with frequency statistics
  - [LexemeKey](../L/LexemeKey.md): Search key structure for binary search
- Constants used:
  - QI_VAL: Query item type for lexeme nodes
  - OP_NOT/OP_AND/OP_PHRASE/OP_OR: Operator type constants
  - DEFAULT_TS_MATCH_SEL: Default selectivity for unknown lexemes
- Macros used:
  - VARSIZE_ANY_EXHDR/VARDATA_ANY: Text data access macros
- Called from (representative examples):
  - [mcelem_tsquery_selec](../m/mcelem_tsquery_selec.md): Entry point with statistics
  - Self-recursive calls for operator evaluation

## Notes and Other Information
- Implements a recursive descent parser for TSQuery expression trees
- Uses probability theory principles for combining operator selectivities
- Handles prefix queries by scanning and extrapolating from MCELEM statistics
- Requires at least 100 MCELEM entries for reliable prefix estimation
- For prefix matches, ensures selectivity is at least as high as exact matches would be
- Uses independent assumption for AND operations (may underestimate for correlated terms)
- Includes comprehensive bounds checking and error handling
- Central to PostgreSQL's cost-based optimization for full-text search queries
- The algorithm assumes that MCELEM statistics are representative of the overall lexeme distribution