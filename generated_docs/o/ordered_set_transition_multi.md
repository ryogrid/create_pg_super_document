# ordered_set_transition_multi

## Location
[src/backend/utils/adt/orderedsetaggs.c:383-426](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/orderedsetaggs.c#L383-L426)

## Overview
Generic transition function for ordered-set aggregates with potentially multiple aggregated input columns, handling tuple-based data collection for complex aggregates.

## Definition


## Detailed Description
The  function serves as the state transition function for ordered-set aggregates that operate on multiple input columns or require tuple-based processing, such as multi-column aggregates and hypothetical-set aggregates like  and . It follows the PostgreSQL aggregate function protocol where the first argument is the aggregate state.

On the first call, it initializes the aggregate state by calling  with  to set up tuple-based sorting. For subsequent calls, it constructs a tuple from all input arguments (excluding the state argument) and stores it in the pre-allocated tuple slot. For hypothetical-set aggregates, it adds a special flag column with value 0 to mark regular input rows (as opposed to the hypothetical row which gets flag value 1).

The complete tuple is then added to the tuplesort object using , and the row counter is incremented. Unlike the single-column version, this function does not filter null values, allowing the final function to handle them as needed.

## Parameters / Member Variables
- : Standard PostgreSQL function arguments macro containing:
  - Argument 0: Current aggregate state (OSAPerGroupState pointer, initially null)
  - Arguments 1+: Input values to be formed into a tuple and added to the sorted collection

## Dependencies
- Functions called/Symbols referenced:
  - [ordered_set_startup](ordered_set_startup.md)
  - ExecClearTuple
  - [ExecStoreVirtualTuple](../E/ExecStoreVirtualTuple.md)
  - [tuplesort_puttupleslot](../t/tuplesort_puttupleslot.md)
  - PG_ARGISNULL
  - PG_GETARG_POINTER
  - PG_GETARG_DATUM
  - PG_NARGS
  - PG_RETURN_POINTER
  - [Int32GetDatum](../I/Int32GetDatum.md)
- Called from (representative examples):
  - PostgreSQL aggregate execution framework for multi-column ordered-set aggregates

## Notes and Other Information
- Uses tuple-based sorting to handle multiple columns and complex sort criteria
- For hypothetical-set aggregates, adds a flag column (0 for regular rows, 1 for hypothetical row)
- Does not filter null values, leaving null handling to the final function
- Maintains running count of rows in 
- Works with aggregates like  or multi-column percentiles
- The tuple slot is reused across calls for efficiency