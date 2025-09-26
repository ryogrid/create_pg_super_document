# GucStack

## Location
[src/include/utils/guc_tables.h:130-152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/guc_tables.h#L130-L152)

## Overview
A typedef alias for the guc_stack structure, representing a stack entry that maintains GUC variable value history for transaction-aware configuration management.

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
    config_var_value masked;       /* SET value in a GUC_SET_LOCAL entry */
} GucStack;
```

## Detailed Description
GucStack is a typedef that creates an alias for the guc_stack structure. This typedef provides a more conventional naming style (PascalCase) for use in contexts where the structure is referenced as a complete type rather than as part of a linked list implementation. The structure implements PostgreSQL's sophisticated GUC variable stack mechanism that handles nested transactions, SET commands, and SET LOCAL commands with proper value restoration semantics.

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
  - Same as guc_stack: GucStackState, GucSource, GucContext, config_var_value
- Used by:
  - [GUCHashEntry](GUCHashEntry.md) (hash table entries for GUC variables)
  - Various GUC management functions (push_old_value, AtEOXact_GUC, reapply_stacked_values)
  - [config_generic](../c/config_generic.md) structure (as part of the GUC variable definition)

## Notes and Other Information
This typedef serves as a cleaner interface name for the guc_stack structure, following PostgreSQL's naming conventions for exported types. It's particularly useful in function signatures and structure member declarations where the full 'struct guc_stack' syntax would be verbose. The functionality is identical to guc_stack - this is purely a naming convenience.