# copy_options

## Location
[src/bin/psql/copy.c:53-64](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/copy.c#L53-L64)

## Overview
A structure that holds parsed options and parameters for psql's \copy command, encapsulating all information needed to execute a copy operation between files and database tables.

## Definition

```c
struct copy_options
{
	char	   *before_tofrom;	/* COPY string before TO/FROM */
	char	   *after_tofrom;	/* COPY string after TO/FROM filename */
	char	   *file;			/* NULL = stdin/stdout */
	bool		program;		/* is 'file' a program to popen? */
	bool		psql_inout;		/* true = use psql stdin/stdout */
	bool		from;			/* true = FROM, false = TO */
};
```
## Detailed Description
The  structure is used by psql to store the parsed components of a \copy command line. It breaks down the complex \copy syntax into manageable components that can be processed by the copy execution logic. The structure handles various forms of the \copy command including table copies, query result copies, and different input/output destinations (files, programs, stdin/stdout).

The structure supports the documented \copy syntax:
-   
- 

It also maintains backward compatibility with pre-7.3 PostgreSQL syntax that allowed BINARY keyword before the table name.

## Parameters / Member Variables
- : Contains the COPY command portion before the TO/FROM keyword, including table name, column list, or query statement
- : Contains any COPY options that appear after the filename (e.g., CSV, DELIMITER, etc.)
- : Filename for the copy operation; NULL indicates stdin/stdout usage
- : Boolean flag indicating whether  represents a program to be executed via popen() rather than a regular file
- : Boolean flag indicating the use of psql's own stdin/stdout (pstdin/pstdout keywords)
- : Direction flag; true for FROM operations (file to table), false for TO operations (table to file)

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a data structure)
- Called from (representative examples):
  -  (creates and populates the structure)
  -  (deallocates the structure)
  -  (uses the structure to execute copy operations)
  -  (used during structure population)

## Notes and Other Information
- The structure is allocated using  to ensure all fields are initially zero/NULL
- String fields (, , ) are allocated separately and must be freed individually
- The  field is initialized to an empty string and built incrementally during parsing
- Supports both regular files and program execution through the  flag
- Handles special psql-specific stdin/stdout redirection via  flag
- Memory management is handled by  function which properly deallocates all string members
- The structure is defined in  and is used exclusively within psql's copy command implementation