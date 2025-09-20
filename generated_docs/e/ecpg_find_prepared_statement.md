# ecpg_find_prepared_statement

## Location
[src/interfaces/ecpg/ecpglib/prepare.c:239-259](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/prepare.c#L239-L259)

## Overview
Searches for a prepared statement by name within a specific database connection's prepared statement list, returning the matching statement and optionally its predecessor in the linked list.

## Definition

```c
struct prepared_statement *
ecpg_find_prepared_statement(const char *name,
							 struct connection *con, struct prepared_statement **prev_)
```
## Detailed Description
This function implements a linear search through a connection's linked list of prepared statements to find a statement with a matching name. It performs a case-sensitive string comparison using  to locate the target prepared statement. The function also provides an optional mechanism to retrieve the predecessor node, which is useful for list manipulation operations such as deletion or insertion.

The function operates on the ECPG (Embedded SQL in C for PostgreSQL) library's internal data structures, where each database connection maintains its own linked list of prepared statements. This enables proper isolation of prepared statements across different database connections.

## Parameters / Member Variables
- : The name of the prepared statement to search for (case-sensitive string comparison)
- : Pointer to the connection structure containing the prepared statements list to search
- : Optional output parameter - if non-NULL, will be set to point to the predecessor node of the found statement (useful for list operations)

## Dependencies
- Functions called/Symbols referenced:
  -  (standard C library function for string comparison)
  -  (struct type for prepared statement nodes)
- Called from (representative examples):
  -  (src/interfaces/ecpg/ecpglib/descriptor.c:869)
  -  (src/interfaces/ecpg/ecpglib/prepare.c:68)
  -  (src/interfaces/ecpg/ecpglib/prepare.c:231)
  -  (src/interfaces/ecpg/ecpglib/prepare.c:325)
  -  (src/interfaces/ecpg/ecpglib/prepare.c:361)
  -  (src/interfaces/ecpg/ecpglib/prepare.c:470)
  -  (src/interfaces/ecpg/ecpglib/prepare.c:572)

## Notes and Other Information
- Returns NULL if no prepared statement with the specified name is found
- The search is performed using case-sensitive string comparison
- Time complexity is O(n) where n is the number of prepared statements in the connection
- The  parameter is optional and can be passed as NULL if the predecessor information is not needed
- This function is essential for prepared statement lifecycle management in ECPG, supporting operations like lookup, deletion, and validation
- The function maintains thread safety by operating only on the provided connection's local prepared statement list
- Each connection maintains its own independent list of prepared statements, preventing cross-connection interference