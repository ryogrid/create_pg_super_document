# GucStackState

## Location
[src/include/utils/guc_tables.h:115-116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/guc_tables.h#L115-L116)

## Overview
GucStackState is an enumeration that defines the different states for GUC (Grand Unified Configuration) stack entries, tracking how configuration parameter values were set during transaction processing.

## Definition

```c
typedef enum
{
	/* This is almost GucAction, but we need a fourth state for SET+LOCAL */
	GUC_SAVE,					/* entry caused by function SET option */
	GUC_SET,					/* entry caused by plain SET command */
	GUC_LOCAL,					/* entry caused by SET LOCAL command */
	GUC_SET_LOCAL,				/* entry caused by SET then SET LOCAL */
} GucStackState;
```
## Detailed Description
GucStackState is used within PostgreSQL's GUC system to track the state of configuration parameter changes in a transactional context. It maintains a stack of previous values that allows proper rollback behavior when transactions abort or when leaving function scope.

The enumeration represents different ways a GUC parameter value can be modified:
- GUC_SAVE: Used when a function with SET options modifies a parameter
- GUC_SET: Used for regular SET commands at transaction level  
- GUC_LOCAL: Used for SET LOCAL commands that only affect the current transaction
- GUC_SET_LOCAL: Used when a SET command is followed by SET LOCAL within the same transaction

This state tracking is essential for PostgreSQL's transactional behavior, ensuring that parameter changes can be properly rolled back and that the scope of changes (global vs transaction-local) is maintained correctly.

## Parameters / Member Variables
- `GUC_SAVE`: Entry caused by function SET option - automatically restored when function exits
- `GUC_SET`: Entry caused by plain SET command - persists beyond transaction boundary
- `GUC_LOCAL`: Entry caused by SET LOCAL command - restored at transaction end
- `GUC_SET_LOCAL`: Entry caused by SET followed by SET LOCAL - complex state for nested changes

## Dependencies
- Functions called/Symbols referenced: None (enum definition)
- Called from (representative examples):
  - [guc_stack](../g/guc_stack.md) struct (src/include/utils/guc_tables.h:121) - uses GucStackState as the 'state' field

## Notes and Other Information
- The comment indicates this is "almost GucAction, but we need a fourth state for SET+LOCAL", showing it's an extension of a simpler state model
- This enum is primarily used in the guc_stack structure to maintain a stack of previous parameter values during nested transactions and function calls
- The distinction between these states is crucial for PostgreSQL's ACID properties, ensuring configuration changes have appropriate scope and durability
- The SET+SET LOCAL combination (GUC_SET_LOCAL) handles the complex case where a transaction-level change is followed by a transaction-local override