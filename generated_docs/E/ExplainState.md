# ExplainState

## Location
src/include/commands/explain.h: 44 - 72

## Overview
The central state structure for PostgreSQL EXPLAIN command execution, containing all configuration options, output formatting state, and contextual information needed to generate query execution plan explanations.

## Definition
```c
typedef struct ExplainState
{
    StringInfo  str;                /* output buffer */
    /* options */
    bool        verbose;            /* be verbose */
    bool        analyze;            /* print actual times */
    bool        costs;              /* print estimated costs */
    bool        buffers;            /* print buffer usage */
    bool        wal;                /* print WAL usage */
    bool        timing;             /* print detailed node timing */
    bool        summary;            /* print total planning and execution timing */
    bool        memory;             /* print planner's memory usage information */
    bool        settings;           /* print modified settings */
    bool        generic;            /* generate a generic plan */
    ExplainSerializeOption serialize;  /* serialize the query's output? */
    ExplainFormat format;           /* output format */
    /* state for output formatting --- not reset for each new plan tree */
    int         indent;             /* current indentation level */
    List       *grouping_stack;     /* format-specific grouping state */
    /* state related to the current plan tree (filled by ExplainPrintPlan) */
    PlannedStmt *pstmt;             /* top of plan */
    List       *rtable;             /* range table */
    List       *rtable_names;       /* alias names for RTEs */
    List       *deparse_cxt;        /* context list for deparsing expressions */
    Bitmapset  *printed_subplans;   /* ids of SubPlans we've printed */
    bool        hide_workers;       /* set if we find an invisible Gather */
    /* state related to the current plan node */
    ExplainWorkersState *workers_state; /* needed if parallel plan */
} ExplainState;
```

## Detailed Description
ExplainState serves as the comprehensive control and state structure for PostgreSQL's EXPLAIN command functionality. It encapsulates all user-specified options (like VERBOSE, ANALYZE, COSTS, etc.), maintains formatting state for different output formats (TEXT, JSON, XML, YAML), and tracks contextual information needed during plan tree traversal.

The structure is designed to support both simple plan explanation and complex analysis with actual execution statistics. When the ANALYZE option is enabled, it coordinates with the query executor to collect and present real performance metrics. For parallel queries, it integrates with ExplainWorkersState to manage per-worker output formatting.

The structure supports multiple output formats through a unified interface, handling format-specific details like indentation (TEXT), grouping (JSON/YAML), and element nesting (XML). It maintains state across the entire explanation process, from initial setup through final output generation.

## Parameters / Member Variables

### Output Buffer
- `str`: Primary output buffer (StringInfo) where the formatted explanation is accumulated

### Option Flags
- `verbose`: Enable verbose output with additional plan details
- `analyze`: Execute the query and show actual runtime statistics
- `costs`: Display estimated costs (startup cost, total cost, etc.)
- `buffers`: Show buffer hit/miss statistics and I/O metrics
- `wal`: Display Write-Ahead Log usage statistics
- `timing`: Show detailed per-node execution timing (requires ANALYZE)
- `summary`: Display total planning and execution time summary
- `memory`: Show planner memory usage information
- `settings`: Display non-default settings that affected planning
- `generic`: Generate a generic plan (for prepared statements)

### Format Control
- `serialize`: Controls query output serialization (ExplainSerializeOption enum)
- `format`: Output format specification (TEXT, JSON, XML, YAML via ExplainFormat enum)

### Formatting State
- `indent`: Current indentation level for TEXT format output
- `grouping_stack`: Stack of format-specific grouping contexts for nested output structures

### Plan Context Information
- `pstmt`: Pointer to the PlannedStmt being explained
- `rtable`: Range table from the query (for resolving table references)
- `rtable_names`: Alias names for range table entries
- `deparse_cxt`: Context list used for deparsing expressions back to SQL text
- `printed_subplans`: Bitmapset tracking which SubPlan IDs have been printed to avoid duplication
- `hide_workers`: Flag indicating presence of invisible Gather nodes

### Parallel Execution Support
- `workers_state`: Pointer to ExplainWorkersState for managing parallel worker output (NULL for non-parallel plans)

## Dependencies

### Functions called/Symbols referenced:
- ExplainSerializeOption (enumeration for serialization options)
- ExplainFormat (enumeration for output format types)
- PlannedStmt (structure representing the planned query)
- ExplainWorkersState (structure for parallel worker output management)
- StringInfo (PostgreSQL string buffer type)
- List (PostgreSQL list type)
- Bitmapset (PostgreSQL bitmap set type)

### Called from (representative examples):
- NewExplainState (creates and initializes new ExplainState)
- ExplainQuery (main entry point for EXPLAIN command)
- ExplainOneQuery (explains a single query)
- ExplainOnePlan (explains a single execution plan)
- ExplainNode (recursive plan node explanation)
- ExplainPrintPlan (prints plan with statistics)
- All show_* functions for displaying specific plan node details

## Notes and Other Information

- The structure is typically allocated via NewExplainState() which sets sensible defaults
- The `costs` option defaults to true, while most other boolean options default to false
- The `workers_state` field is only allocated when explaining parallel plans
- Format-specific state management is handled through the `grouping_stack` mechanism
- The structure supports both planning-only explanation (fast) and execution analysis (slower but more informative)
- Thread-safe when used in single-threaded context; parallel worker coordination is handled through workers_state
- Memory management follows PostgreSQL palloc conventions with automatic cleanup on context destruction
- The structure supports extension through the formatting framework without breaking existing functionality