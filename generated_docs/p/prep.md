# prep

## Location
[src/interfaces/ecpg/preproc/type.h:107-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/type.h#L107-L113)

## Overview
The  structure represents a prepared statement in the ECPG (Embedded SQL in C) system, storing essential information about prepared SQL statements including the statement name, SQL text, and type information.

## Definition

```c
struct prep
{
	char	   *name;
	char	   *stmt;
	char	   *type;
};
```
## Detailed Description
 is a fundamental data structure in PostgreSQL's ECPG preprocessor and runtime library that encapsulates the information needed to manage prepared SQL statements. It serves as a container for the three key components of a prepared statement: its identifier name, the actual SQL statement text, and type information. This structure is actively used in the ECPG library for statement preparation, execution, and descriptor management operations.

## Parameters / Member Variables
- : A character pointer containing the unique identifier name for the prepared statement
- : A character pointer containing the actual SQL statement text to be prepared and executed
- : A character pointer containing type information associated with the prepared statement, likely used for parameter and result type checking

## Dependencies
- Functions called/Symbols referenced:
  - No direct symbol references from this structure
- Called from (representative examples):
  - [ECPGdescribe](../E/ECPGdescribe.md) (in src/interfaces/ecpg/ecpglib/descriptor.c)
  - [ecpg_auto_prepare](../e/ecpg_auto_prepare.md) (in src/interfaces/ecpg/ecpglib/prepare.c)

## Notes and Other Information
- This structure is central to ECPG's prepared statement functionality, located in 
- Used extensively in descriptor operations and automatic statement preparation
- The structure uses simple character pointers, requiring careful memory management
- Multiple references found in both descriptor.c and prepare.c indicate its importance in ECPG's statement lifecycle management