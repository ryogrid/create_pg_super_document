# describe

## Location
[src/interfaces/ecpg/preproc/type.h:228-234](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/type.h#L228-L234)

## Overview
A structure used in ECPG (Embedded C for PostgreSQL) preprocessor to store information about DESCRIBE statement operations during SQL preprocessing.

## Definition

```c
struct describe
{
	int			input;
	char	   *stmt_name;
};
```
## Detailed Description
The  struct is part of the ECPG preprocessor infrastructure, designed to handle DESCRIBE statements in embedded SQL. DESCRIBE is a SQL statement that provides metadata about prepared statements or result sets. This structure appears to capture the essential parameters needed when processing DESCRIBE operations during the preprocessing phase.

The structure contains an integer flag to indicate the type of describe operation (likely distinguishing between input and output descriptions) and a statement name string that identifies which prepared statement is being described. This allows the ECPG preprocessor to generate appropriate C code that will perform the describe operation at runtime.

## Parameters / Member Variables
- : Integer flag indicating the type of describe operation, likely distinguishing between input parameter description (non-zero) and output column description (zero)
- : Pointer to a string containing the name of the prepared statement to be described

## Dependencies
- Functions called/Symbols referenced: None identified
- Called from (representative examples): No specific references found in the analyzed codebase

## Notes and Other Information
- Located in the ECPG preprocessor type definitions (src/interfaces/ecpg/preproc/type.h:228-234)
- Final structure defined in the type.h header file
- Related to SQL DESCRIBE statements which provide metadata about prepared statements
- Part of the ECPG's support for dynamic SQL operations
- The  flag likely corresponds to SQL standard DESCRIBE INPUT vs DESCRIBE OUTPUT operations
- DESCRIBE INPUT provides information about the parameters expected by a prepared statement
- DESCRIBE OUTPUT provides information about the columns that will be returned by a prepared statement
- Simple structure suggesting it serves as a parameter holder for describe-related preprocessing operations
- The lack of extensive references may indicate specialized usage or that it's part of a less commonly used SQL feature path