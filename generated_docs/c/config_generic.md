# config_generic

## Location
src/include/utils/guc_tables.h: 153 - 186

## Overview
The base structure containing common fields for all types of GUC (Grand Unified Configuration) variables in PostgreSQL's configuration system.

## Definition
```c
struct config_generic
{
    /* constant fields, must be set correctly in initial value: */
    const char *name;           /* name of variable - MUST BE FIRST */
    GucContext  context;        /* context required to set the variable */
    enum config_group group;    /* to help organize variables by function */
    const char *short_desc;     /* short desc. of this variable's purpose */
    const char *long_desc;      /* long desc. of this variable's purpose */
    int         flags;          /* flag bits, see guc.h */
    /* variable fields, initialized at runtime: */
    enum config_type vartype;   /* type of variable (set only at startup) */
    int         status;         /* status bits, see below */
    GucSource   source;         /* source of the current actual value */
    GucSource   reset_source;   /* source of the reset_value */
    GucContext  scontext;       /* context that set the current value */
    GucContext  reset_scontext; /* context that set the reset value */
    Oid         srole;          /* role that set the current value */
    Oid         reset_srole;    /* role that set the reset value */
    GucStack   *stack;          /* stacked prior values */
    void       *extra;          /* "extra" pointer for current actual value */
    dlist_node  nondef_link;    /* list link for variables that have source
                                 * different from PGC_S_DEFAULT */
    slist_node  stack_link;     /* list link for variables that have non-NULL
                                 * stack */
    slist_node  report_link;    /* list link for variables that have the
                                 * GUC_NEEDS_REPORT bit set in status */
    char       *last_reported;  /* if variable is GUC_REPORT, value last sent
                                 * to client (NULL if not yet sent) */
    char       *sourcefile;     /* file current setting is from (NULL if not
                                 * set in config file) */
    int         sourceline;     /* line in source file */
};
```

## Detailed Description
The config_generic structure serves as the common foundation for all PostgreSQL GUC variables, regardless of their specific data type (bool, int, real, string, enum). It contains metadata about the variable's definition, runtime state, value sources, transaction context, and links for various maintenance operations. This structure is embedded as the first member in all specific config variable types (config_bool, config_int, etc.), enabling polymorphic operations through pointer casting. The design supports sophisticated features like transaction rollback, SET LOCAL semantics, configuration file tracking, and client reporting.

## Parameters / Member Variables
### Constant Fields (set at initialization):
- `name`: Variable name - must be first field for polymorphic access
- `context`: Context required to modify the variable (e.g., superuser, session)
- `group`: Logical grouping for organization (e.g., logging, performance)
- `short_desc`: Brief description (under 80 chars) for help displays
- `long_desc`: Detailed description for comprehensive documentation
- `flags`: Various behavioral flags (see guc.h for definitions)

### Runtime Fields:
- `vartype`: Type of the variable (bool, int, real, string, enum)
- `status`: Status bits indicating current state and special conditions
- `source`: Source of the current value (file, command line, SET command, etc.)
- `reset_source`: Source of the reset value for RESET command
- `scontext`: Context that set the current value
- `reset_scontext`: Context that set the reset value
- `srole`: Role OID that set the current value
- `reset_srole`: Role OID that set the reset value
- `stack`: Pointer to stack of previous values for transaction rollback
- `extra`: Opaque data pointer for variable-specific extensions
- `nondef_link`: Link for list of variables with non-default sources
- `stack_link`: Link for list of variables with active stacks
- `report_link`: Link for list of variables needing client reporting
- `last_reported`: Last value reported to client (for GUC_REPORT variables)
- `sourcefile`: Configuration file where current value was set
- `sourceline`: Line number in the configuration file

## Dependencies
- Types referenced:
  - GucContext (enum for variable contexts)
  - config_group (enum for variable groupings)
  - config_type (enum for variable data types)
  - GucSource (enum for value sources)
  - GucStack (stack structure for transaction handling)
  - dlist_node, slist_node (list node structures)
- Used by:
  - All specific config variable types (config_bool, config_int, config_real, config_string, config_enum)
  - Extensive usage throughout the GUC system for variable management, validation, and reporting

## Notes and Other Information
This structure is the backbone of PostgreSQL's GUC system, providing a unified interface for managing configuration variables of different types. The polymorphic design allows the same code to handle different variable types by casting pointers to config_generic. The sourcefile and sourceline fields are kept in this base structure rather than in the stack to avoid bloating stack entries, as they're only relevant for file-sourced values. The various link fields support efficient maintenance operations by organizing variables into linked lists based on their current state.