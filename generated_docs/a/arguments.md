# arguments

## Location
[src/interfaces/ecpg/preproc/type.h:196-202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/type.h#L196-L202)

## Overview
A linked list structure used in ECPG (Embedded C for PostgreSQL) to represent function arguments and their associated indicator variables during SQL preprocessing.

## Definition


## Detailed Description
The  struct is part of the ECPG preprocessor infrastructure, designed to manage SQL function arguments in embedded C programs. It forms a linked list where each node contains a primary variable and an optional indicator variable. This structure is essential for handling SQL function calls where parameters may have associated null indicators or status information.

The structure supports the ECPG's need to track both the actual data variable and its corresponding indicator variable, which is used to detect null values or provide status information about the parameter. The linked list design allows for handling functions with variable numbers of arguments.

## Parameters / Member Variables
- : Pointer to the primary variable structure containing the actual argument data, type information, and memory details
- : Pointer to the indicator variable structure used for null detection and status information (can be NULL if no indicator is needed)
- : Pointer to the next arguments node in the linked list, enabling support for multiple function parameters

## Dependencies
- Functions called/Symbols referenced:
  -  (struct type from ecpglib_extern.h)
- Called from (representative examples):
  -  (variable.c:305, 363, 364)
  -  (variable.c:377, 379)
  -  (variable.c:389, 391, 392)
  -  (variable.c:407, 409)
  -  (variable.c:436)
  -  struct (type.h:143-146)

## Notes and Other Information
- Located in the ECPG preprocessor module (src/interfaces/ecpg/preproc/type.h:196-202)
- Part of the embedded SQL preprocessing infrastructure
- Used extensively in catalog cache operations and function result set execution
- The indicator variable is optional and may be NULL for arguments that don't require null status tracking
- Forms part of a larger ecosystem of ECPG data structures for SQL-to-C translation