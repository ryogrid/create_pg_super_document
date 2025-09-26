# QueryEnvironment

## Location
[src/backend/utils/misc/queryenvironment.c:32-38](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/queryenvironment.c#L32-L38)

## Overview
QueryEnvironment is an opaque data structure that provides a query execution context for managing ephemeral named relations (ENRs), particularly for temporary relations like trigger transition tables and named tuplestores that exist only during query execution.

## Definition

```c
struct QueryEnvironment
{
	List	   *namedRelList;
};
```
## Detailed Description
The QueryEnvironment serves as a container for ephemeral named relations that are not stored in the system catalogs but need to be accessible during query parsing, planning, and execution. The structure is intentionally opaque outside of queryenvironment.c to allow implementation changes without affecting existing code.

The primary use case is for managing named tuplestores containing delta information from "normal" relations, such as:
- Trigger transition tables (OLD/NEW tables in AFTER triggers)
- Temporary result sets used in complex queries
- Intermediate data structures during query processing

The implementation uses a simple linked list (List) because the number of ephemeral relations in any context is expected to be very small. If performance becomes an issue, the implementation can be changed transparently since the structure is opaque to callers.

## Parameters / Member Variables
- : A PostgreSQL List containing EphemeralNamedRelation objects. Each entry represents a named temporary relation that can be referenced during query execution.

## Dependencies

### Functions called/Symbols referenced:
-  (PostgreSQL list data structure)
-  (structure for ephemeral relation data)
-  (PostgreSQL memory allocator)
-  (list append function)
-  (list deletion function)
-  (list element access)
-  (string comparison)
-  /  (relation access functions)

### Called from (representative examples):
- [Query](Query.md) parsing functions ()
- [Query](Query.md) execution (, )
- [Plan](../P/Plan.md) caching (, ) 
- Utility commands (, )
- SPI (Server Programming Interface) operations
- [Trigger](../T/Trigger.md) execution contexts
- [Portal](../P/Portal.md) management

## Related Functions
The QueryEnvironment is manipulated through these interface functions:

- : Creates and initializes a new QueryEnvironment
- : Registers an ephemeral named relation in the environment
- : Removes an ephemeral named relation by name
- : Retrieves an ephemeral named relation by name
- : Gets metadata for a visible ephemeral named relation
- : Extracts tuple descriptor from ENR metadata

## Notes and Other Information
- The structure is designed to be opaque to promote encapsulation and allow implementation changes
- Currently implemented as a simple list due to expected small number of entries per context
- Used extensively in trigger processing for OLD/NEW transition tables
- Critical for proper handling of ephemeral relations in cached plans and DDL operations
- The ENR system allows temporary relations to participate in query planning without being stored in system catalogs
- Performance consideration: Linear search through the list is acceptable given small expected size
- Memory management: ENRs are typically allocated in query-specific memory contexts