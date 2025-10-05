# jsonb_subscript_fetch_old

## Location
[src/backend/utils/adt/jsonbsubs.c:323-352](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonbsubs.c#L323-L352)

## Overview
Computes the old JSONB element value for SubscriptingRef assignment expressions that contain nested SubscriptingRef or FieldStore operations.

## Definition

```c
static void
jsonb_subscript_fetch_old(ExprState *state,
						  ExprEvalStep *op,
						  ExprContext *econtext)
```
## Detailed Description
This function is a specialized variant of the regular JSONB fetch operation, used specifically in assignment contexts where the new-value subexpression contains SubscriptingRef or FieldStore operations. It fetches the current value of a JSONB element before assignment, which may be needed for complex assignment expressions.

The key differences from the regular fetch operation are:
1. **NULL Handling**: Can handle NULL source JSONB containers (unlike regular fetch which assumes non-NULL due to fetch_strict)
2. **Storage Location**: Stores the result in the SubscriptingRefState's prevvalue/prevnull fields instead of the main result area
3. **Purpose**: Provides the "before" value for assignment operations that need to reference the existing value

The function directly uses the original subscript indices from the SubscriptingRefState rather than the processed workspace indices, which is appropriate for this specialized use case.

## Parameters / Member Variables
- `*state`: Expression evaluation state (not directly used in this function)
- `*op`: Expression evaluation step containing the SubscriptingRefState
- `*econtext`: Expression context for evaluation (not directly used in this function)
## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetJsonbP](../D/DatumGetJsonbP.md)
  - [jsonb_get_element](jsonb_get_element.md)
- Called from:
  - [jsonb_exec_setup](jsonb_exec_setup.md)

## Notes and Other Information
- Only called when the new-value subexpression contains SubscriptingRef or FieldStore
- Handles NULL source JSONB by setting prevvalue to 0 and prevnull to true
- Uses sbsrefstate->upperindex directly instead of workspace->index (processed subscripts)
- Stores results in prevvalue/prevnull fields of SubscriptingRefState for later use
- Part of the complex assignment expression evaluation framework
- The false parameter passed to jsonb_get_element likely controls extraction behavior similar to the regular fetch function

## Simplified Source

```c
static void jsonb_subscript_fetch_old(ExprState *state,
                                     ExprEvalStep *op,
                                     ExprContext *econtext) {
    SubscriptingRefState *sbsrefstate = op->d.sbsref.state;

    if (*op->resnull) {
        // Source JSONB is null, so element is also null
        sbsrefstate->prevvalue = (Datum) 0;
        sbsrefstate->prevnull = true;
    } else {
        // Extract old element value for assignment operations
        Jsonb *jsonbSource = DatumGetJsonbP(*op->resvalue);

        sbsrefstate->prevvalue = jsonb_get_element(jsonbSource,
                                                 sbsrefstate->upperindex,
                                                 sbsrefstate->numupper,
                                                 &sbsrefstate->prevnull,
                                                 false);
    }
}
```