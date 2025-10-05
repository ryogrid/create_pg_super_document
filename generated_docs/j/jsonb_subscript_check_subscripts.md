# jsonb_subscript_check_subscripts

## Location
[src/backend/utils/adt/jsonbsubs.c:175-234](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonbsubs.c#L175-L234)

## Overview
Processes and validates subscripts in a SubscriptingRef expression during execution, converting them to the appropriate format for JSONB operations.

## Definition

```c
struct
	 * an empty source.
	 */
	if (sbsrefstate->numupper > 0 && sbsrefstate->upperprovided[0] &&
		!sbsrefstate->upperindexnull[0] && workspace->indexOid[0] == INT4OID)
		workspace->expectArray = true;
```
## Detailed Description
This function is called during the execution phase to process subscripts that have already been evaluated to Datum form. It performs several critical tasks:

1. **Type Detection**: Determines if the first subscript is an integer, which indicates the source JSONB should be treated as an array
2. **NULL Handling**: Checks for NULL subscripts and either throws an error (in assignment contexts) or sets the result to NULL (in fetch contexts)
3. **Type Conversion**: Converts integer subscripts to text format using int4out function, since JSONB operations internally work with text-based paths
4. **Workspace Setup**: Populates the JsonbSubWorkspace structure with processed subscript values

The function sets the  flag when the first subscript is an integer, which helps guide the behavior of subsequent JSONB operations when the source value is NULL.

## Parameters / Member Variables
- : Expression evaluation state (not directly used in this function)
- : Expression evaluation step containing the SubscriptingRefState in op->d.sbsref_subscript.state
- : Expression context for evaluation (not directly used in this function)

## Dependencies
- Functions called/Symbols referenced:
  - [int4out](../i/int4out.md)
  - DirectFunctionCall1
  - [DatumGetCString](../D/DatumGetCString.md)
  - CStringGetTextDatum
  - ereport
- Called from:
  - [jsonb_exec_setup](jsonb_exec_setup.md)

## Notes and Other Information
- Returns  if any subscript is NULL in a fetch context, instructing the caller to skip the rest of the SubscriptingRef sequence
- Throws an error if any subscript is NULL in an assignment context
- [Integer](../I/Integer.md) subscripts are converted to text format using the int4out function for internal consistency
- The  flag is set when the first subscript is an integer, helping to determine the expected JSONB container type
- Text subscripts are used as-is without conversion
- The function assumes subscripts have already been evaluated and type-checked during the transform phase

## Simplified Source

```c
static bool jsonb_subscript_check_subscripts(ExprState *state,
                                            ExprEvalStep *op,
                                            ExprContext *econtext) {
    SubscriptingRefState *sbsrefstate = op->d.sbsref_subscript.state;
    JsonbSubWorkspace *workspace = (JsonbSubWorkspace *) sbsrefstate->workspace;

    // Check if first subscript is integer -> expect array
    if (sbsrefstate->numupper > 0 && sbsrefstate->upperprovided[0] &&
        !sbsrefstate->upperindexnull[0] && workspace->indexOid[0] == INT4OID) {
        workspace->expectArray = true;
    }

    // Process each upper subscript
    for (int i = 0; i < sbsrefstate->numupper; i++) {
        if (sbsrefstate->upperprovided[i]) {
            // Handle NULL subscripts
            if (sbsrefstate->upperindexnull[i]) {
                if (sbsrefstate->isassignment) {
                    ereport(ERROR,
                        (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                         errmsg("jsonb subscript in assignment must not be null")));
                }
                *op->resnull = true;
                return false;
            }

            // Convert integer subscripts to text format
            if (workspace->indexOid[i] == INT4OID) {
                Datum datum = sbsrefstate->upperindex[i];
                char *cs = DatumGetCString(DirectFunctionCall1(int4out, datum));
                workspace->index[i] = CStringGetTextDatum(cs);
            } else {
                // Text subscripts used as-is
                workspace->index[i] = sbsrefstate->upperindex[i];
            }
        }
    }

    return true;
}
```