# AtEOSubXact_Parallel

## Location
[src/backend/access/transam/parallel.c:1250-1270](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/parallel.c#L1250-L1270)

## Overview
Performs end-of-subtransaction cleanup for parallel contexts by destroying any parallel contexts that were initiated within the current subtransaction.

## Definition


## Detailed Description
This function is called during subtransaction cleanup (both commit and abort) to properly manage parallel contexts that were created within the subtransaction being ended. It walks through the global list of parallel contexts (pcxt_list) and destroys any contexts whose subtransaction ID matches the current subtransaction.

The function processes contexts in LIFO (Last In, First Out) order by always examining the head of the list. When a parallel context with a matching subtransaction ID is found, it is destroyed using DestroyParallelContext(). The function continues until it encounters a context from a different subtransaction or the list becomes empty.

If the subtransaction is committing and parallel contexts still exist, this indicates a resource leak, and a WARNING is logged. In a properly functioning system, all parallel contexts should be explicitly destroyed before subtransaction commit.

The function ensures proper cleanup during both successful subtransaction commits and aborts, preventing resource leaks and maintaining system stability.

## Parameters / Member Variables
- : Boolean flag indicating whether this is a subtransaction commit (true) or abort (false)
- : The SubTransactionId of the subtransaction being ended, used to identify which parallel contexts to clean up

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_is_empty](../d/dlist_is_empty.md)
  - dlist_head_element
  - DestroyParallelContext
  - elog

- Called from (representative examples):
  - [CommitSubTransaction](../C/CommitSubTransaction.md)
  - [AbortSubTransaction](AbortSubTransaction.md)
  - IsParallelWorker (referenced in header)

## Notes and Other Information
- This function is part of the subtransaction cleanup infrastructure
- Parallel contexts are tracked per subtransaction to enable proper cleanup
- The WARNING message for leaked contexts helps identify programming errors
- Contexts are processed in reverse creation order (LIFO) due to list structure
- The function is called for both commit and abort scenarios but behaves differently
- Proper parallel context management is crucial for avoiding resource leaks in complex transaction scenarios