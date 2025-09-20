# PsqlScanStateData

## Location
[src/include/fe_utils/psqlscan_int.h:84-132](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/fe_utils/psqlscan_int.h#L84-L132)

## Overview
PsqlScanStateData is the central state structure that contains all working state of PostgreSQL's lexical scanner, enabling re-entrant and multi-instance lexer operations for psql and related utilities.

## Definition

```c
typedef struct PsqlScanStateData
{
	yyscan_t	scanner;		/* Flex's state for this PsqlScanState */

	PQExpBuffer output_buf;		/* current output buffer */

	StackElem  *buffer_stack;	/* stack of variable expansion buffers */

	/*
	 * These variables always refer to the outer buffer, never to any stacked
	 * variable-expansion buffer.
	 */
	YY_BUFFER_STATE scanbufhandle;
	char	   *scanbuf;		/* start of outer-level input buffer */
	const char *scanline;		/* current input line at outer level */

	/* safe_encoding, curline, refline are used by emit() to replace FFs */
	int			encoding;		/* encoding being used now */
	bool		safe_encoding;	/* is current encoding "safe"? */
	bool		std_strings;	/* are string literals standard? */
	const char *curline;		/* actual flex input string for cur buf */
	const char *refline;		/* original data for cur buffer */

	/*
	 * All this state lives across successive input lines, until explicitly
	 * reset by psql_scan_reset.  start_state is adopted by yylex() on entry,
	 * and updated with its finishing state on exit.
	 */
	int			start_state;	/* yylex's starting/finishing state */
	int			state_before_str_stop;	/* start cond. before end quote */
	int			paren_depth;	/* depth of nesting in parentheses */
	int			xcdepth;		/* depth of nesting in slash-star comments */
	char	   *dolqstart;		/* current $foo$ quote start string */

	/*
	 * State to track boundaries of BEGIN ... END blocks in function
	 * definitions, so that semicolons do not send query too early.
	 */
	int			identifier_count;	/* identifiers since start of statement */
	char		identifiers[4]; /* records the first few identifiers */
	int			begin_depth;	/* depth of begin/end pairs */

	/*
	 * Callback functions provided by the program making use of the lexer,
	 * plus a void* callback passthrough argument.
	 */
	const PsqlScanCallbacks *callbacks;
	void	   *cb_passthrough;
} PsqlScanStateData;
```
## Detailed Description
PsqlScanStateData is the comprehensive state container for PostgreSQL's lexical scanning system, designed to support re-entrant lexer operations essential for handling nested include files and multiple simultaneous scanning contexts. This structure encapsulates all the information needed to maintain scanning state between lexer calls, enabling sophisticated features like variable substitution, multibyte encoding support, and complex SQL parsing rules.

The structure is organized into several logical groups: flex integration state, buffer management for variable expansion, encoding handling for international character sets, persistent parsing state for complex SQL constructs, and callback mechanisms for extensibility. This design allows psql to handle complex scenarios like nested variable expansions, dollar-quoted strings, and proper parsing of stored procedure definitions with BEGIN/END blocks.

## Parameters / Member Variables
- : yyscan_t holding flex's internal state for this particular scanner instance
- : PQExpBuffer for accumulating the current output being constructed
- : StackElem pointer to the top of the variable expansion buffer stack
- : YY_BUFFER_STATE for the outer-level input buffer (not variable expansion buffers)
- : Pointer to the start of the outer-level input buffer string
- : Current input line being processed at the outer level
- : Integer identifier for the character encoding currently in use
- : Boolean indicating whether the current encoding is "safe" (all bytes >= 0x80)
- : Boolean indicating whether string literals follow standard SQL rules
- : Actual string data that flex is scanning from the current buffer
- : Original unmodified data for the current buffer (before FF substitutions)
- : Integer representing yylex's starting and finishing state for persistence
- : State condition before encountering an end quote
- : Current nesting depth within parentheses
- : Current nesting depth within /* */ style comments
- : String holding the current dollar-quote delimiter (e.g., "foo" for $foo$)
- : Number of identifiers seen since the start of the current statement
- : Array storing the first few identifiers to detect BEGIN/END patterns
- : Current nesting depth of BEGIN/END block pairs
- : Pointer to callback functions for variable resolution and other extensions
- : Void pointer passed through to callback functions for context

## Dependencies
- Functions called/Symbols referenced:
  - yyscan_t (flex scanner type)
  - PQExpBuffer (PostgreSQL string buffer type)
  - [StackElem](../S/StackElem.md) (variable expansion stack element)
  - [YY_BUFFER_STATE](../Y/YY_BUFFER_STATE.md) (flex buffer state)
  - [PsqlScanCallbacks](PsqlScanCallbacks.md) (callback function structure)
- Called from (representative examples):
  - [PsqlScanState](PsqlScanState.md) (typedef alias for this structure)
  - psqlscan_create (creates instances)
  - psqlscan_destroy (destroys instances)

## Notes and Other Information
- Essential for supporting re-entrant lexer operations and multiple simultaneous scanning contexts
- Handles complex multibyte encoding scenarios by substituting 0xFF for unsafe bytes during scanning
- Supports sophisticated SQL parsing including dollar-quoted strings and nested comments
- Manages variable expansion through a stack-based buffer system
- Designed to work with multiple compatible lexers (psqlscan.l, psqlscanslash.l) that can share state
- The structure must persist between lexer calls to maintain parsing context across input lines
- Critical for proper handling of PostgreSQL-specific SQL extensions and psql meta-commands
- Used exclusively in frontend utilities, not in the backend database server