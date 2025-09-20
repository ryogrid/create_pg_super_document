# SharedDependencyObjectType

## Location
[src/backend/catalog/pg_shdepend.c:72-78](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_shdepend.c#L72-L78)

## Overview
An enum type that classifies the nature of database objects in PostgreSQL's shared dependency tracking system, distinguishing between local, shared, and remote objects.

## Definition

```c
typedef struct
{
	ObjectAddress object;
	char		deptype;
	SharedDependencyObjectType objtype;
} ShDependObjectInfo;
```
## Detailed Description
The  enum is used within PostgreSQL's shared dependency system to categorize database objects based on their scope and accessibility. This classification is crucial for the dependency tracking mechanism that ensures referential integrity across the database cluster. The enum helps determine how dependencies should be handled during operations like object dropping, reassignment, and dependency validation.

The enum is primarily used in conjunction with the shared dependency catalog () to track relationships between database objects that can span multiple databases within a PostgreSQL cluster.

## Parameters / Member Variables
- : Represents objects that are local to a specific database and not shared across the cluster
- : Represents objects that are shared across the entire PostgreSQL cluster (e.g., roles, tablespaces)
- : Represents objects that exist in remote databases, used for cross-database dependency tracking

## Dependencies
- Functions called/Symbols referenced:
  - Used as a member type in  struct
- Called from (representative examples):
  -  function for formatting dependency descriptions
  - Used in dependency tracking and validation operations

## Notes and Other Information
- The enum is defined in  at lines 67-72
- Each enum value has specific handling logic in the  function for generating appropriate dependency descriptions
-  and  are handled similarly in most contexts, while  has special handling for counting multiple objects in remote databases
- This type is essential for PostgreSQL's multi-database architecture where some objects (like roles) are cluster-wide while others are database-specific