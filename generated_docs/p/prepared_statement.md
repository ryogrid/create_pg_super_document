# prepared_statement

## Location
[src/interfaces/ecpg/ecpglib/ecpglib_extern.h:95-103](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/ecpglib_extern.h#L95-L103)

## Overview
A structure that manages prepared SQL statements in ECPG, maintaining a linked list of prepared statements associated with database connections.

## Definition

```c
struct prepared_statement
{
	char	   *name;
	bool		prepared;
	struct statement *stmt;
	struct prepared_statement *next;
};
```
## Detailed Description
The prepared_statement structure implements a linked list-based registry for managing prepared SQL statements within ECPG connections. This structure serves as a wrapper around the core statement structure, adding preparation-specific metadata and organization capabilities. It enables ECPG to track which statements have been prepared on the server side, avoid duplicate preparations, and manage the lifecycle of prepared statements efficiently.

The structure supports ECPG's prepared statement functionality by maintaining the preparation state and providing a mechanism to locate prepared statements by name. This design allows for efficient reuse of prepared statements and proper cleanup when connections are closed or statements are deallocated.

## Parameters / Member Variables
- `*name`: String identifier for the prepared statement, used for lookups and server-side references
- `prepared`: Boolean flag indicating whether this statement has been successfully prepared on the database server
- `*stmt`: Pointer to the underlying statement structure containing the SQL command and execution details
- `*next`: Pointer to the next prepared statement in the linked list, enabling chaining of multiple prepared statements per connection
## Dependencies
- Functions called/Symbols referenced:
  - [statement](../s/statement.md) (core statement structure for SQL execution details)
  - [prepared_statement](prepared_statement.md) (self-reference for linked list structure)
- Called from (representative examples):
  - [ECPGdescribe](../E/ECPGdescribe.md) (src/interfaces/ecpg/ecpglib/descriptor.c:851)
  - [connection](../c/connection.md) (src/interfaces/ecpg/ecpglib/ecpglib_extern.h:110)
  - [ecpg_register_prepared_stmt](../e/ecpg_register_prepared_stmt.md) (src/interfaces/ecpg/ecpglib/prepare.c:62-73)
  - [prepare_common](prepare_common.md) (src/interfaces/ecpg/ecpglib/prepare.c:162-166)
  - [ECPGprepare](../E/ECPGprepare.md) (src/interfaces/ecpg/ecpglib/prepare.c:221-238)
  - [ecpg_find_prepared_statement](../e/ecpg_find_prepared_statement.md) (src/interfaces/ecpg/ecpglib/prepare.c:240-242)
  - [deallocate_one](../d/deallocate_one.md) (src/interfaces/ecpg/ecpglib/prepare.c:261)
  - [ECPGdeallocate](../E/ECPGdeallocate.md) (src/interfaces/ecpg/ecpglib/prepare.c:318)
  - [ecpg_auto_prepare](../e/ecpg_auto_prepare.md) (src/interfaces/ecpg/ecpglib/prepare.c:565)

## Notes and Other Information
- Implements a simple linked list structure for organizing prepared statements per database connection
- The 'prepared' flag helps avoid redundant server-side preparation calls and tracks preparation status
- Used extensively in ECPG's automatic statement preparation and caching mechanisms
- Memory management requires careful handling during connection cleanup and statement deallocation
- The structure enables efficient statement lookup by name, supporting both explicit and automatic preparation workflows
- Integrates with ECPG's statement caching system to optimize repeated SQL execution performance