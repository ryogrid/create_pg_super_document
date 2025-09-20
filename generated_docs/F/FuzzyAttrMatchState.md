# FuzzyAttrMatchState

## Location
[src/backend/parser/parse_relation.c:73-74](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L73-L74)

## Overview
A state tracking structure used during column name resolution to find fuzzy matches for misspelled or closely-named column references, supporting PostgreSQL's intelligent error reporting and suggestion system.

## Definition

```c
typedef struct
{
	int			distance;		/* Current or limit distance */
	RangeTblEntry *rfirst;		/* RTE of closest non-exact match, or NULL */
	AttrNumber	first;			/* Col index in rfirst */
	RangeTblEntry *rsecond;		/* RTE of another non-exact match w/same dist */
	AttrNumber	second;			/* Col index in rsecond */
	RangeTblEntry *rexact1;		/* RTE of first exact match, or NULL */
	AttrNumber	exact1;			/* Col index in rexact1 */
	RangeTblEntry *rexact2;		/* RTE of second exact match, or NULL */
	AttrNumber	exact2;			/* Col index in rexact2 */
} FuzzyAttrMatchState;
```
## Detailed Description
FuzzyAttrMatchState is a data structure used by PostgreSQL's parser to track fuzzy attribute matching during column name resolution. When a column reference cannot be found exactly, this structure maintains state about potential matches based on Levenshtein distance calculations. The system uses this information to provide helpful error messages suggesting similar column names when SQL queries contain typos or minor naming errors.

The structure separates tracking of exact matches (when multiple tables have columns with the same name) from fuzzy matches (when column names are similar but not identical). For fuzzy matching, it maintains the distance metric and tracks up to two matches at the same distance level. If three or more columns are found at the same distance, the system considers that distance too broad to be useful for suggestions and clears the fuzzy match candidates.

The maximum acceptable fuzzy distance is controlled by MAX_FUZZY_DISTANCE (defined as 3), and matches are rejected if more than half the characters differ from the target name to avoid ridiculous suggestions.

## Parameters / Member Variables
- : Current best fuzzy-match distance if rfirst isn't NULL, otherwise maximum acceptable distance plus 1
- : RangeTblEntry pointer to the relation containing the closest non-exact match, or NULL if none found
- : Column index (AttrNumber) within rfirst for the closest non-exact match
- : RangeTblEntry pointer to another relation with a non-exact match at exactly the same distance as rfirst
- : Column index (AttrNumber) within rsecond for the second non-exact match
- : RangeTblEntry pointer to the first relation containing an exact match, or NULL if none
- : Column index (AttrNumber) within rexact1 for the first exact match
- : RangeTblEntry pointer to a second relation containing an exact match, or NULL if none
- : Column index (AttrNumber) within rexact2 for the second exact match

## Dependencies
- Functions called/Symbols referenced:
  - AttrNumber (type from catalog/pg_attribute.h)
  - [RangeTblEntry](../R/RangeTblEntry.md) (type from parsenodes.h)
  - MAX_FUZZY_DISTANCE (constant, value 3)

- Called from (representative examples):
  - [updateFuzzyAttrMatchState](../u/updateFuzzyAttrMatchState.md) - Updates the state with new potential matches
  - [scanRTEForColumn](../s/scanRTEForColumn.md) - Scans a single RTE for column matches and updates fuzzy state
  - [colNameToVar](../c/colNameToVar.md) - Resolves column references to Var nodes
  - [searchRangeTableForCol](../s/searchRangeTableForCol.md) - Searches range table for column references
  - [errorMissingColumn](../e/errorMissingColumn.md) - Uses fuzzy state to generate helpful error messages

## Notes and Other Information
- The fuzzy matching algorithm uses Levenshtein distance with equal weights (1,1,1) for insertions, deletions, and substitutions
- Exact matches are tracked independently of fuzzy matches to handle ambiguous column references
- When three or more fuzzy matches are found at the same distance, the system clears rfirst/rsecond to avoid unhelpful suggestions
- Dropped columns (appearing as empty strings) are explicitly rejected during fuzzy matching
- The structure supports PostgreSQL's goal of providing helpful error messages when column references fail
- Used primarily in src/backend/parser/parse_relation.c for query parsing and column resolution