# guc_stack

## Location
[src/include/utils/guc_tables.h:117-129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/guc_tables.h#L117-L129)

## Overview
A stack structure that maintains the history of GUC (Grand Unified Configuration) variable values, supporting nested transactions and SET/SET LOCAL commands.

## Definition
```c
typedef struct guc_stack
{
    struct guc_stack *prev;        /* previous stack item, if any */
    int             nest_level;     /* nesting depth at which we made entry */
    GucStackState   state;         /* see enum above */
    GucSource       source;        /* source of the prior value */
    /* masked value's source must be PGC_S_SESSION, so no need to store it */
    GucContext      scontext;      /* context that set the prior value */
    GucContext      masked_scontext; /* context that set the masked value */
    Oid             srole;         /* role that set the prior value */
    Oid             masked_srole;  /* role that set the masked value */
    config_var_value prior;        /* previous value of variable */
    config_var_value masked;       /* SET value, when SET LOCAL occurred */
} guc_stack;
```

## Detailed Description
The guc_stack structure implements a stack-based mechanism for managing GUC variable values across nested transactions and SET/SET LOCAL command combinations. Each stack entry preserves the state of a GUC variable at a particular nesting level, allowing for proper rollback and restoration of values when transactions abort or complete. The stack supports the complex semantics of PostgreSQL's SET and SET LOCAL commands, where SET LOCAL creates a temporary override that is automatically reverted at transaction end.

## Parameters / Member Variables
- `prev`: Pointer to the previous stack entry, forming a linked list of saved states
- `nest_level`: The transaction nesting depth when this stack entry was created
- `state`: The type of operation that created this stack entry (GUC_SAVE, GUC_SET, GUC_LOCAL, or GUC_SET_LOCAL)
- `source`: The source that set the prior value (e.g., configuration file, command line, etc.)
- `scontext`: The context in which the prior value was set
- `masked_scontext`: The context in which a masked value was set (for SET LOCAL scenarios)
- `srole`: The role ID that set the prior value
- `masked_srole`: The role ID that set the masked value
- `prior`: The previous value of the GUC variable before this stack entry
- `masked`: The SET value when SET LOCAL occurred, stored for later restoration

## Dependencies
- Types referenced:
  - [guc_stack](guc_stack.md) (self-reference for linked list structure)
  - [GucStackState](../G/GucStackState.md) (enum indicating the type of stack operation)
  - GucSource (enum indicating the source of the configuration value)
  - GucContext (enum indicating the context of the configuration setting)
  - [config_var_value](../c/config_var_value.md) (structure containing the actual variable value and extra data)
- Used by:
  - Various GUC management functions in the PostgreSQL configuration system

## Notes and Other Information
This structure is crucial for implementing PostgreSQL's sophisticated transaction-aware configuration management. The stack allows for proper handling of nested transactions where configuration changes must be rolled back if inner transactions abort. The distinction between 'prior' and 'masked' values enables the complex semantics of SET LOCAL, where a value can be temporarily overridden within a transaction while preserving the ability to restore both the original value and any intermediate SET value.