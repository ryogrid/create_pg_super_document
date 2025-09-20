# TS_execute_locations_recurse

## Location
[src/backend/utils/adt/tsvector_op.c:2025-2155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L2025-L2155)

## Overview
A recursive function that executes text search query evaluation while tracking position locations, handling operators above any phrase operator in the query tree.

## Definition

```c
struct from each
				 * combination of sub-matches, following the disjunctive law
				 * (A & B) | (C & D) = (A | C) & (A | D) & (B | C) & (B | D).
				 *
				 * However, if either input didn't produce locations (i.e., it
				 * failed or was a NOT), we must just return the other list.
				 */
				if (llocations == NIL)
					*locations = rlocations;
```
## Detailed Description
This function implements the core recursive logic for evaluating text search queries while maintaining location information for matching terms. It traverses the query tree structure, handling different query operators (NOT, AND, OR, PHRASE) and collecting position data for matches. The function is specifically designed to work with operators above phrase operators, delegating phrase-specific operations to TS_phrase_execute.

The function uses a callback mechanism (chkcond) to test individual query operands and builds location lists that track where matches occur in the text. For OR operations, it implements the disjunctive law to generate all possible combinations of locations from sub-matches.

## Parameters / Member Variables
- : Pointer to the current QueryItem being processed in the query tree
- : Generic argument passed to the callback function for operand checking
- : Callback function that tests whether a query operand matches
- : Output parameter that receives a list of ExecPhraseData structures containing match locations

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth
  - palloc0_object
  - list_make1
  - [list_concat](../l/list_concat.md)
  - lappend
  - TS_phrase_execute
  - TS_phrase_output
- Called from (representative examples):
  - TS_execute_locations
  - [TS_execute_locations_recurse](TS_execute_locations_recurse.md) (recursive calls)

## Notes and Other Information
- Includes stack overflow protection via check_stack_depth() calls
- Implements query cancellation checking with CHECK_FOR_INTERRUPTS()
- For OR operations, uses the disjunctive law: (A & B) | (C & D) = (A | C) & (A | D) & (B | C) & (B | D)
- Returns empty location list by default, populated only when matches are found
- Handles special cases where operands don't produce locations (failures or NOT operations)