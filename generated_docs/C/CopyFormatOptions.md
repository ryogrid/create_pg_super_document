# CopyFormatOptions

## Location
[src/include/commands/copy.h:57-87](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/commands/copy.h#L57-L87)

## Overview
CopyFormatOptions is a struct that holds parsed COPY command options related to formatting and behavior, serving as a centralized configuration structure for PostgreSQL's COPY functionality.

## Definition

```c
typedef struct CopyFormatOptions
{
	/* parameters from the COPY command */
	int			file_encoding;	/* file or remote side's character encoding,
								 * -1 if not specified */
	bool		binary;			/* binary format? */
	bool		freeze;			/* freeze rows on loading? */
	bool		csv_mode;		/* Comma Separated Value format? */
	CopyHeaderChoice header_line;	/* header line? */
	char	   *null_print;		/* NULL marker string (server encoding!) */
	int			null_print_len; /* length of same */
	char	   *null_print_client;	/* same converted to file encoding */
	char	   *default_print;	/* DEFAULT marker string */
	int			default_print_len;	/* length of same */
	char	   *delim;			/* column delimiter (must be 1 byte) */
	char	   *quote;			/* CSV quote char (must be 1 byte) */
	char	   *escape;			/* CSV escape char (must be 1 byte) */
	List	   *force_quote;	/* list of column names */
	bool		force_quote_all;	/* FORCE_QUOTE *? */
	bool	   *force_quote_flags;	/* per-column CSV FQ flags */
	List	   *force_notnull;	/* list of column names */
	bool		force_notnull_all;	/* FORCE_NOT_NULL *? */
	bool	   *force_notnull_flags;	/* per-column CSV FNN flags */
	List	   *force_null;		/* list of column names */
	bool		force_null_all; /* FORCE_NULL *? */
	bool	   *force_null_flags;	/* per-column CSV FN flags */
	bool		convert_selectively;	/* do selective binary conversion? */
	CopyOnErrorChoice on_error; /* what to do when error happened */
	CopyLogVerbosityChoice log_verbosity;	/* verbosity of logged messages */
	List	   *convert_select; /* list of column names (can be NIL) */
} CopyFormatOptions;
```
## Detailed Description
CopyFormatOptions encapsulates all formatting and behavioral parameters for PostgreSQL's COPY command, which is used for bulk data import/export operations. This struct consolidates various options including format specifications (binary vs text vs CSV), character encoding settings, delimiter and quote characters, NULL handling, error handling behavior, and column-specific formatting flags. The structure is designed to be populated during COPY command parsing and then passed to the actual copy implementation functions.

## Parameters / Member Variables
- : Character encoding of the file or remote side (-1 if not specified)
- : Flag indicating whether to use binary format instead of text
- : Flag to freeze rows during loading for performance optimization
- : Flag indicating Comma Separated Value format
- : Enum specifying header line handling behavior
- : String representation of NULL values in server encoding
- : Length of the NULL marker string
- : NULL marker string converted to file encoding
- : String representation of DEFAULT values
- : Length of the DEFAULT marker string
- : Column delimiter character (must be single byte)
- : CSV quote character (must be single byte)
- : CSV escape character (must be single byte)
- : List of column names to always quote in CSV mode
- : Flag to force quoting of all columns
- : Per-column flags for forced quoting
- : List of column names to never treat as NULL
- : Flag to apply FORCE_NOT_NULL to all columns
- : Per-column flags for FORCE_NOT_NULL behavior
- : List of column names to treat as NULL when empty
- : Flag to apply FORCE_NULL to all columns
- : Per-column flags for FORCE_NULL behavior
- : Flag for selective binary conversion
- : Enum specifying error handling behavior
- : Enum specifying verbosity level for logged messages
- : List of column names for selective conversion

## Dependencies
- Functions called/Symbols referenced:
  - CopyHeaderChoice (enum for header line options)
  - CopyOnErrorChoice (enum for error handling behavior)
  - CopyLogVerbosityChoice (enum for logging verbosity)
- Called from (representative examples):
  - [ProcessCopyOptions](../P/ProcessCopyOptions.md) (populates this struct from COPY command options)
  - [CopyToState](CopyToState.md) (uses this struct for COPY TO operations)
  - [CopyFromStateData](CopyFromStateData.md) (uses this struct for COPY FROM operations)

## Notes and Other Information
This struct serves as the central configuration hub for all COPY operations in PostgreSQL. While most members relate to formatting, the  option is noted in the source comments as not truly belonging here but being parsed along with other options for convenience. The structure supports both text and binary formats, with extensive CSV-specific options for fine-grained control over column behavior. The per-column flag arrays enable different formatting rules for individual columns within the same COPY operation.