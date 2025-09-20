# core_yy_extra_type

## Location
[src/include/parser/scanner.h:66-116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/parser/scanner.h#L66-L116)

## Overview
The core scanner's private state structure that contains all necessary data for lexical analysis in PostgreSQL's SQL parser, designed to be embedded as the first component of larger scanner state structures.

## Definition

```c
typedef struct core_yy_extra_type
{
	/*
	 * The string the scanner is physically scanning.  We keep this mainly so
	 * that we can cheaply compute the offset of the current token (yytext).
	 */
	char	   *scanbuf;
	Size		scanbuflen;

	/*
	 * The keyword list to use, and the associated grammar token codes.
	 */
	const ScanKeywordList *keywordlist;
	const uint16 *keyword_tokens;

	/*
	 * Scanner settings to use.  These are initialized from the corresponding
	 * GUC variables by scanner_init().  Callers can modify them after
	 * scanner_init() if they don't want the scanner's behavior to follow the
	 * prevailing GUC settings.
	 */
	int			backslash_quote;
	bool		escape_string_warning;
	bool		standard_conforming_strings;

	/*
	 * literalbuf is used to accumulate literal values when multiple rules are
	 * needed to parse a single literal.  Call startlit() to reset buffer to
	 * empty, addlit() to add text.  NOTE: the string in literalbuf is NOT
	 * necessarily null-terminated, but there always IS room to add a trailing
	 * null at offset literallen.  We store a null only when we need it.
	 */
	char	   *literalbuf;		/* palloc'd expandable buffer */
	int			literallen;		/* actual current string length */
	int			literalalloc;	/* current allocated buffer size */

	/*
	 * Random assorted scanner state.
	 */
	int			state_before_str_stop;	/* start cond. before end quote */
	int			xcdepth;		/* depth of nesting in slash-star comments */
	char	   *dolqstart;		/* current $foo$ quote start string */
	YYLTYPE		save_yylloc;	/* one-element stack for PUSH_YYLLOC() */

	/* first part of UTF16 surrogate pair for Unicode escapes */
	int32		utf16_first_part;

	/* state variables for literal-lexing warnings */
	bool		warn_on_first_escape;
	bool		saw_non_ascii;
} core_yy_extra_type;
```
## Detailed Description
This structure serves as the YY_EXTRA data that a flex scanner allows to be passed around during lexical analysis. It contains all the private state needed by PostgreSQL's core scanner. The structure is designed to be extensible - the actual yy_extra struct in calling parsers may be larger and have this as its first component, allowing parser-specific fields to be added while maintaining compatibility with the core scanner functionality.

The structure manages various aspects of SQL tokenization including buffer management, keyword recognition, string literal processing, comment handling, and Unicode escape sequences. It also maintains scanner settings that can be initialized from GUC variables and modified by callers to control scanner behavior.

## Parameters / Member Variables
- `*scanbuf`: The string buffer that the scanner is physically scanning, used for computing token offsets
- `scanbuflen`: Length of the scan buffer
- `*keywordlist`: Pointer to the keyword list used for token recognition
- `*keyword_tokens`: Associated grammar token codes for keywords
- `backslash_quote`: Scanner setting for backslash quote handling (from GUC variables)
- `escape_string_warning`: Setting to control escape string warnings
- `standard_conforming_strings`: Setting for standard conforming string behavior
- `*literalbuf`: Expandable buffer for accumulating literal values during multi-rule parsing
- `literallen`: Current actual length of the literal string
- `literalalloc`: Current allocated size of the literal buffer
- `state_before_str_stop`: Start condition before encountering end quote
- `xcdepth`: Nesting depth in slash-star comments
- `*dolqstart`: Current dollar-quote start string (e.g., $foo$)
- `save_yylloc`: One-element stack for PUSH_YYLLOC() macro operations
- `utf16_first_part`: First part of UTF16 surrogate pair for Unicode escapes
- `warn_on_first_escape`: State variable for literal-lexing warnings
- `saw_non_ascii`: State variable tracking non-ASCII characters in literals
## Dependencies
- Functions called/Symbols referenced:
  - ScanKeywordList
  - YYLTYPE
- Called from (representative examples):
  - [base_yy_extra_type](../b/base_yy_extra_type.md)

## Notes and Other Information
This structure is fundamental to PostgreSQL's lexical analysis system and must be carefully maintained to ensure proper SQL parsing. The literalbuf mechanism allows for efficient handling of complex string literals that require multiple lexer rules. The dollar-quote support enables PostgreSQL's extended string quoting functionality. Scanner settings can be modified after initialization to override GUC-based defaults when needed.