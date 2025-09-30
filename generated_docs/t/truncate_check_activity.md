# truncate_check_activity

## Location
[src/backend/commands/tablecmds.c:2368-2390](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L2368-L2390)

## Overview
A static utility function that performs extra sanity checks to determine if a given relation is safe to truncate, focusing on activity-based restrictions.

## Definition
static void truncate_check_activity(Relation rel)

## Detailed Description
This function implements activity-based safety checks for table truncation operations. It is part of a two-phase validation system, working alongside truncate_check_rel() to ensure truncation operations are safe. The function specifically checks for two critical conditions that would make truncation unsafe:

1. Cross-session temporary table protection: Prevents truncation of temporary tables belonging to other database sessions, as their local buffer managers cannot handle the operation properly.

2. Current transaction activity detection: Identifies active uses of the relation within the current transaction, including open table scans and pending AFTER trigger events that could be disrupted by truncation.

The function is designed as a separate validation step because it requires an already-opened Relation object, unlike the callback-based checks that operate on relation names.

## Parameters / Member Variables
- rel: The Relation object representing the table to be checked for truncation safety

## Dependencies
- Functions called/Symbols referenced:
  - RELATION_IS_OTHER_TEMP (macro for detecting other sessions temp tables)
  - [CheckTableNotInUse](../C/CheckTableNotInUse.md) (function to detect active table usage)
  - ereport (error reporting function)
- Called from (representative examples):
  - [ExecuteTruncate](../E/ExecuteTruncate.md) (main truncate execution function)
  - [ExecuteTruncateGuts](../E/ExecuteTruncateGuts.md) (internal truncate processing function)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the tablecmds.c compilation unit
- The function is split from other truncation checks because it requires an open Relation, while callback-based checks like RangeVarCallbackForTruncate() cannot open Relations yet
- Errors thrown by this function will abort the truncation operation with appropriate error codes and messages
- The function is part of PostgreSQL's comprehensive safety framework for DDL operations

## Simplified Source

```c
static void truncate_check_activity(Relation rel) {
    // Don't allow truncating temp tables from other sessions
    if (RELATION_IS_OTHER_TEMP(rel)) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("cannot truncate temporary tables of other sessions")));
    }

    // Check for active uses in current transaction
    // (open scans, pending AFTER triggers, etc.)
    CheckTableNotInUse(rel, "TRUNCATE");
}
```