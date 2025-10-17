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
  - [check_stack_depth](../c/check_stack_depth.md)
  - palloc0_object
  - list_make1
  - [list_concat](../l/list_concat.md)
  - [lappend](../l/lappend.md)
  - [TS_phrase_execute](TS_phrase_execute.md)
  - TS_phrase_output
- Called from (representative examples):
  - [TS_execute_locations](TS_execute_locations.md)
  - [TS_execute_locations_recurse](TS_execute_locations_recurse.md) (recursive calls)

## Notes and Other Information
- Includes stack overflow protection via check_stack_depth() calls
- Implements query cancellation checking with CHECK_FOR_INTERRUPTS()
- For OR operations, uses the disjunctive law: (A & B) | (C & D) = (A | C) & (A | D) & (B | C) & (B | D)
- Returns empty location list by default, populated only when matches are found
- Handles special cases where operands don't produce locations (failures or NOT operations)

## Simplified Source

```c
static bool TS_execute_locations_recurse(QueryItem *curitem, void *arg,
                                        TSExecuteCallback chkcond, List **locations) {
    bool lmatch, rmatch;
    List *llocations, *rlocations;
    ExecPhraseData *data;

    // Safety checks
    check_stack_depth();
    CHECK_FOR_INTERRUPTS();

    // Default: no locations found
    *locations = NIL;

    // Base case: evaluate leaf operand
    if (curitem->type == QI_VAL) {
        data = palloc0_object(ExecPhraseData);
        if (chkcond(arg, (QueryOperand *) curitem, data) == TS_YES) {
            *locations = list_make1(data);
            return true;
        }
        pfree(data);
        return false;
    }

    // Handle different operators
    switch (curitem->qoperator.oper) {
        case OP_NOT:
            // NOT: succeed if operand fails, but don't return any locations
            if (!TS_execute_locations_recurse(curitem + 1, arg, chkcond, &llocations))
                return true;  // NOT succeeds, but no locations to report
            return false;

        case OP_AND:
            // AND: both operands must succeed
            if (!TS_execute_locations_recurse(curitem + curitem->qoperator.left,
                                             arg, chkcond, &llocations))
                return false;
            if (!TS_execute_locations_recurse(curitem + 1, arg, chkcond, &rlocations))
                return false;

            // Concatenate location lists
            *locations = list_concat(llocations, rlocations);
            return true;

        case OP_OR:
            // OR: at least one operand must succeed
            lmatch = TS_execute_locations_recurse(curitem + curitem->qoperator.left,
                                                 arg, chkcond, &llocations);
            rmatch = TS_execute_locations_recurse(curitem + 1, arg, chkcond, &rlocations);

            if (lmatch || rmatch) {
                // Handle location combination for OR
                if (llocations == NIL)
                    *locations = rlocations;
                else if (rlocations == NIL)
                    *locations = llocations;
                else {
                    // Apply disjunctive law: generate combinations
                    // (A & B) | (C & D) = (A | C) & (A | D) & (B | C) & (B | D)
                    ListCell *ll;
                    foreach(ll, llocations) {
                        ExecPhraseData *ldata = (ExecPhraseData *) lfirst(ll);
                        ListCell *lr;
                        foreach(lr, rlocations) {
                            ExecPhraseData *rdata = (ExecPhraseData *) lfirst(lr);

                            data = palloc0_object(ExecPhraseData);
                            TS_phrase_output(data, ldata, rdata,
                                           TSPO_BOTH | TSPO_L_ONLY | TSPO_R_ONLY,
                                           0, 0, ldata->npos + rdata->npos);
                            // Report larger width for OR operations
                            data->width = Max(ldata->width, rdata->width);
                            *locations = lappend(*locations, data);
                        }
                    }
                }
                return true;
            }
            return false;

        case OP_PHRASE:
            // Delegate to phrase execution
            data = palloc0_object(ExecPhraseData);
            if (TS_phrase_execute(curitem, arg, TS_EXEC_EMPTY, chkcond, data) == TS_YES) {
                if (!data->negate)
                    *locations = list_make1(data);
                return true;
            }
            pfree(data);
            return false;

        default:
            elog(ERROR, "unrecognized operator: %d", curitem->qoperator.oper);
    }

    return false;
}
```