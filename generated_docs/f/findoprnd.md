# findoprnd

## Location
[src/backend/utils/adt/tsquery.c:784-816](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery.c#L784-L816)

## Overview
A wrapper function that initiates the process of filling in left-offset fields for tsquery operators and detecting stop words that require cleanup.

## Definition


## Detailed Description
The findoprnd function serves as the entry point for processing a tsquery structure in polish notation to compute operator left-offset fields and detect stop words. It initializes the traversal position and needcleanup flag, then delegates the actual work to findoprnd_recurse. After the recursive traversal completes, it performs a validation check to ensure all nodes in the array were processed correctly. If the final position doesn't match the expected size, it indicates a malformed tsquery structure and raises an error. This function is essential for converting a parsed tsquery into its final executable form with properly computed operator offsets.

## Parameters / Member Variables
- `ptr`: Pointer to an array of QueryItem structures representing the tsquery in polish notation
- `size`: Total number of QueryItem elements in the array
- `needcleanup`: Pointer to boolean flag that will be set if stop words are found requiring cleanup

## Dependencies
- Functions called/Symbols referenced:
  - QueryItem (tsquery node structure)
  - [findoprnd_recurse](findoprnd_recurse.md) (recursive traversal function)
  - elog (error reporting function for malformed queries)
- Called from (representative examples):
  - [parse_tsquery](../p/parse_tsquery.md) (during query processing)
  - [tsqueryrecv](../t/tsqueryrecv.md) (during deserialization)

## Notes and Other Information
- Acts as a simple wrapper around findoprnd_recurse with initialization and validation
- Performs important validation to detect malformed tsquery structures
- The needcleanup output parameter informs callers whether stop word removal is needed
- Essential step in tsquery processing pipeline between parsing and execution
- Ensures structural integrity of the tsquery before it can be used for text searching
- The validation check helps catch internal errors in query construction or transmission