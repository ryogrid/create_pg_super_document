# brin_desummarize_range

## Location
[src/backend/access/brin/brin.c:1482-1571](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L1482-L1571)

## Overview
A SQL-callable function that removes summary information for a specific block range in a BRIN index, effectively marking that range as no longer summarized.

## Definition

```c
Datum
brin_desummarize_range(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides the opposite functionality to  by removing existing summary tuples for a specified block range. It performs similar validation as the summarize function, ensuring the target is a valid BRIN index and that the user has appropriate permissions. Unlike , this function is never called by autovacuum, so it doesn't need to switch security contexts. The actual work is delegated to , which handles the low-level details of removing summary entries from the revmap. The function uses a loop to ensure the operation completes successfully.

## Parameters / Member Variables
- : The OID of the BRIN index from which to remove summaries (accessed via )
- : The specific block number to desummarize (accessed via )

## Dependencies
- Functions called/Symbols referenced:
  - : Checks if database recovery is ongoing
  - : Used for block number range validation
  - : Gets the heap relation OID from index OID
  - /: Opens relations with appropriate locks
  - : Gets current user ID for permission checking
  - : Verifies ownership permissions
  - : Reports permission errors
  - : Performs the actual desummarization work
  - : Closes relations and releases locks
  - : Lock level used for operations
  - : Returns void since no value is returned
- Called from (representative examples):
  - SQL interface (as this is a SQL-callable function)

## Notes and Other Information
- Blocks operation during recovery with the same error message as other BRIN control functions
- Validates block number range against  (not  like summarize)
- Does not support the special  value - only works on specific block ranges
- Unlike , never called by autovacuum so no security context switching needed
- Uses the same deadlock avoidance strategy (lock table before index)
- Requires table ownership for execution (same as VACUUM privileges)
- Returns void since there's no meaningful return value
- Uses a do-while loop to ensure  completes successfully
- Only processes valid indexes (indisvalid must be true)
- The actual tuple removal is handled by the revmap subsystem