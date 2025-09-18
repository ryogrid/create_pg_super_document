# TS_execute_locations_recurse

## Location
src/backend/utils/adt/tsvector_op.c: 2025 - 2155

## Overview
A recursive function that executes text search query evaluation while tracking position locations, handling operators above any phrase operator in the query tree.

## Definition


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
  - list_concat
  - lappend
  - TS_phrase_execute
  - TS_phrase_output
- Called from (representative examples):
  - TS_execute_locations
  - TS_execute_locations_recurse (recursive calls)

## Notes and Other Information
- Includes stack overflow protection via check_stack_depth() calls
- Implements query cancellation checking with CHECK_FOR_INTERRUPTS()
- For OR operations, uses the disjunctive law: (A & B) | (C & D) = (A | C) & (A | D) & (B | C) & (B | D)
- Returns empty location list by default, populated only when matches are found
- Handles special cases where operands don't produce locations (failures or NOT operations)