# set_output_count

## Location
[src/backend/executor/nodeSetOp.c:150-189](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSetOp.c#L150-L189)

## Overview
Determines how many copies of a tuple group's representative row should be emitted based on SQL set operation semantics and the duplicate counts from left and right inputs.

## Definition

```c
static void
set_output_count(SetOpState *setopstate, SetOpStatePerGroup pergroup)
```
## Detailed Description
This function implements the core logic for SQL set operations by calculating the number of output tuples that should be produced for a completed tuple group. It follows the SQL92 specification for set operations, examining the duplicate counts from both left and right input relations and applying the appropriate set operation rules.

The function uses a switch statement to handle four different set operation types:
- INTERSECT: Outputs 1 tuple if both sides have duplicates, 0 otherwise
- INTERSECT ALL: Outputs the minimum count between left and right sides
- EXCEPT: Outputs 1 tuple if left side has duplicates but right side doesn't, 0 otherwise  
- EXCEPT ALL: Outputs the difference (left count minus right count), or 0 if negative

The calculated count is stored in setopstate->numOutput for use by the tuple emission logic.

## Parameters / Member Variables
- : Pointer to SetOpState execution state where the output count will be stored
- : Pointer to SetOpStatePerGroup containing the duplicate counts (numLeft, numRight) for the current tuple group

## Dependencies
- Functions called/Symbols referenced:
  - SetOpState (execution state structure)
  - SetOpStatePerGroup (per-group counting structure)
  - SetOp (plan node structure)
  - SETOPCMD_INTERSECT, SETOPCMD_INTERSECT_ALL (command constants)
  - SETOPCMD_EXCEPT, SETOPCMD_EXCEPT_ALL (command constants)
  - elog (error logging function)
- Called from (representative examples):
  - setop_retrieve_direct
  - setop_retrieve_hash_table

## Notes and Other Information
- Implements SQL92 set operation semantics precisely
- The logic handles both regular set operations (INTERSECT, EXCEPT) and ALL variants
- Uses defensive programming with an error case for unrecognized set operation commands
- Critical for correct implementation of SQL set operations in PostgreSQL
- The output count determines how many times the representative tuple will be emitted to the parent node