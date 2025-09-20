# cnfa

## Location
[src/include/regex/regguts.h:406-410](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/regex/regguts.h#L406-L410)

## Overview
The  struct represents a compiled NFA (Non-deterministic Finite Automaton) used in PostgreSQL's regular expression engine for efficient pattern matching.

## Definition

```c
struct cnfa
{
	int			nstates;		/* number of states */
	int			ncolors;		/* number of colors (max color in use + 1) */
	int			flags;			/* bitmask of the following flags: */
#define  HASLACONS	01			/* uses lookaround constraints */
#define  MATCHALL	02			/* matches all strings of a range of lengths */
#define  HASCANTMATCH 04		/* contains CANTMATCH arcs */
	/* Note: HASCANTMATCH appears in nfa structs' flags, but never in cnfas */
	int			pre;			/* setup state number */
	int			post;			/* teardown state number */
	color		bos[2];			/* colors, if any, assigned to BOS and BOL */
	color		eos[2];			/* colors, if any, assigned to EOS and EOL */
	char	   *stflags;		/* vector of per-state flags bytes */
#define  CNFA_NOPROGRESS	01	/* flag bit for a no-progress state */
	struct carc **states;		/* vector of pointers to outarc lists */
	/* states[n] are pointers into a single malloc'd array of arcs */
	struct carc *arcs;			/* the area for the lists */
	/* these fields are used only in a MATCHALL NFA (else they're -1): */
	int			minmatchall;	/* min number of chrs to match */
	int			maxmatchall;	/* max number of chrs to match, or DUPINF */
};
```
## Detailed Description
The  struct is the core data structure representing a compiled NFA in PostgreSQL's regex engine. It stores the complete state machine representation of a regular expression pattern after compilation. The structure includes state information, transition arcs between states, color mappings for character classes, and various optimization flags. This compiled form enables efficient pattern matching during regex execution by providing a direct representation of the automaton's transitions and states.

## Parameters / Member Variables
- `nstates`: Total number of states in the NFA
- `ncolors`: Number of distinct colors used for character classification (maximum color value + 1)
- `flags`: Bitmask containing behavioral flags:
  - `HASLACONS`: Indicates the NFA uses lookaround constraints (lookahead/lookbehind)
  - `MATCHALL`: Indicates the NFA matches all strings within a specific length range
  - `HASCANTMATCH`: Indicates presence of CANTMATCH arcs (used in NFA but not in compiled CNFA)
- `pre`: State number for the initial setup state
- `post`: State number for the final teardown state
- `bos[2]`: Colors assigned to Beginning-Of-String (BOS) and Beginning-Of-Line (BOL) anchors
- `eos[2]`: Colors assigned to End-Of-String (EOS) and End-Of-Line (EOL) anchors
- `stflags`: Byte array containing per-state flags (e.g., CNFA_NOPROGRESS for no-progress states)
- `states`: Array of pointers to outgoing arc lists for each state
- `arcs`: Memory area containing all transition arcs in a single allocation
- `minmatchall`: Minimum character count to match (used only for MATCHALL NFAs, -1 otherwise)
- `maxmatchall`: Maximum character count to match or DUPINF for unlimited (used only for MATCHALL NFAs, -1 otherwise)

## Dependencies
- Functions called/Symbols referenced:
  - struct carc (for transition arcs)
  - color (for character color classification)
- Called from (representative examples):
  - [compact](compact.md) (NFA compilation)
  - [freecnfa](../f/freecnfa.md) (memory cleanup)
  - [dumpcnfa](../d/dumpcnfa.md) (debugging output)
  - [longest](../l/longest.md)/shortest (pattern matching functions)
  - [newdfa](../n/newdfa.md) (DFA construction)
  - [pg_reg_getnumstates](../p/pg_reg_getnumstates.md) (export functions)

## Notes and Other Information
- The CNFA is the compiled, optimized form of an NFA used for actual pattern matching execution
- State transitions are represented through the  (compiled arc) structures pointed to by the  array
- The color system is used to group equivalent characters together for more efficient processing
- The structure supports both standard regex matching and specialized MATCHALL patterns for length-based matching
- Memory is managed efficiently with  containing all transition data in a single allocation and  providing indexed access
- The structure is central to PostgreSQL's regex engine performance, enabling fast pattern matching operations