# _SPI_plan

## Location
src/include/executor/spi_priv.h: 90 - 103

## Overview
The  struct is the core data structure representing execution plans in PostgreSQL's Server Programming Interface (SPI). It encapsulates prepared SQL statements with their associated metadata, argument types, and execution context for efficient query execution and reuse.

## Definition

```c
typedef struct _SPI_plan
{
	int			magic;			/* should equal _SPI_PLAN_MAGIC */
	bool		saved;			/* saved or unsaved plan? */
	bool		oneshot;		/* one-shot plan? */
	List	   *plancache_list; /* one CachedPlanSource per parsetree */
	MemoryContext plancxt;		/* Context containing _SPI_plan and data */
	RawParseMode parse_mode;	/* raw_parser() mode */
	int			cursor_options; /* Cursor options used for planning */
	int			nargs;			/* number of plan arguments */
	Oid		   *argtypes;		/* Argument types (NULL if nargs is 0) */
	ParserSetupHook parserSetup;	/* alternative parameter spec method */
	void	   *parserSetupArg;
} _SPI_plan;
```
## Detailed Description
The  structure manages the lifecycle and execution state of prepared SQL statements within the SPI framework. It supports three distinct plan states:

1. **Saved Plans**: Persist until explicitly destroyed, with memory contexts under CacheMemoryContext and plancache entries that respond to invalidation events
2. **Unsaved Plans**: Exist only within the SPI procedure context and disappear at function exit, with plancache entries that don't respond to invalidation events
3. **Temporary Plans**: Have no dedicated memory context (plancxt == NULL) and exist as local variables with loose memory allocation

The structure also supports "one-shot" plans optimized for single execution, where CachedPlanSources remain incomplete until execution time. Plans handle edge cases like whitespace-only queries by maintaining argument type arrays even when plancache_list is empty.

## Parameters / Member Variables
- : Magic number validation field (should equal _SPI_PLAN_MAGIC)
- : Boolean flag indicating whether the plan is saved (persistent) or unsaved (temporary)
- : Boolean flag marking plans intended for single execution with specific optimizations
- : List of CachedPlanSource objects, one per parsed statement tree
- : Memory context containing the _SPI_plan structure and associated data (NULL for temporary plans)
- : Parsing mode used by raw_parser() for SQL statement processing
- : Options flags used during query planning for cursor operations
- : Count of plan arguments/parameters
- : Array of PostgreSQL type OIDs for plan arguments (NULL when nargs is 0)
- : Hook function pointer for alternative parameter specification methods
- : Argument passed to the parserSetup hook function

## Dependencies
- Functions called/Symbols referenced:
  - RawParseMode (enum type for parsing modes)
  - List (PostgreSQL list data structure)
  - MemoryContext (PostgreSQL memory management)
  - Oid (PostgreSQL object identifier type)
  - ParserSetupHook (function pointer type)
  - CachedPlanSource (plancache system structures)

- Called from (representative examples):
  - SPI_execute (query execution functions)
  - SPI_prepare_extended (plan preparation functions)
  - SPI_cursor_open_with_args (cursor operations)
  - _SPI_make_plan_non_temp (plan state management)
  - _SPI_save_plan (plan persistence functions)
  - SPIPlanPtr (public typedef pointer)

## Notes and Other Information
- The structure is opaque to standard SPI users, who interact with it through the SPIPlanPtr typedef
- Memory management varies significantly based on plan type (saved, unsaved, temporary)
- One-shot plans use various optimizations assuming single execution and may have incomplete CachedPlanSources
- The magic field provides runtime validation and corruption detection
- Plans with whitespace-only queries have empty plancache_list but retain argument type information
- Integration with PostgreSQL's plancache system provides automatic invalidation handling for saved plans
- Located in src/include/executor/spi_priv.h:90-103 as a private SPI implementation detail