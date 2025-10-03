# CreateTableAsRelExists

## Location
[src/backend/commands/createas.c:386-432](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/createas.c#L386-L432)

## Overview
Utility function that checks whether the target relation for a CREATE TABLE AS statement already exists, handling IF NOT EXISTS logic and extension membership validation.

## Definition

```c
bool
CreateTableAsRelExists(CreateTableAsStmt *ctas)
```
## Detailed Description
The  function provides comprehensive existence checking for CREATE TABLE AS and CREATE MATERIALIZED VIEW statements. It resolves the target namespace, checks for existing relations with the same name, and implements the complete IF NOT EXISTS semantics including appropriate error reporting and security validation.

When a relation already exists, the function handles two scenarios:
1. If IF NOT EXISTS was not specified, it raises a DUPLICATE_TABLE error
2. If IF NOT EXISTS was specified, it performs extension membership validation (for security), issues a notice about skipping creation, and returns true

The function also validates that pre-existing objects are members of the current extension when running in an extension script context, preventing security risks from object confusion.

## Parameters / Member Variables
- `*ctas`: CreateTableAsStmt structure containing the complete statement information including the target relation specification and IF NOT EXISTS flag
## Dependencies
- Functions called/Symbols referenced:
  - [RangeVarGetCreationNamespace](../R/RangeVarGetCreationNamespace.md)
  - [get_relname_relid](../g/get_relname_relid.md)
  - ObjectAddressSet
  - [checkMembershipInCurrentExtension](../c/checkMembershipInCurrentExtension.md)
- Called from (representative examples):
  - [ExecCreateTableAs](../E/ExecCreateTableAs.md)
  - [ExplainOneUtility](../E/ExplainOneUtility.md)

## Notes and Other Information
- This is a public function exported through createas.h for use by explain and other subsystems
- Implements security validation through extension membership checking
- Provides consistent error messages and notice handling for relation existence scenarios
- Used by EXPLAIN to determine whether a CREATE TABLE AS would succeed without executing it
- Returns boolean to indicate whether creation should be skipped (true) or proceeded (false)
- Handles both regular CREATE TABLE AS and CREATE MATERIALIZED VIEW statements uniformly

## Simplified Source

```c
bool
CreateTableAsRelExists(CreateTableAsStmt *ctas)
{
    Oid nspid;
    Oid oldrelid;
    ObjectAddress address;
    IntoClause *into = ctas->into;

    // Get the target namespace for the new relation
    nspid = RangeVarGetCreationNamespace(into->rel);

    // Check if relation already exists in that namespace
    oldrelid = get_relname_relid(into->rel->relname, nspid);

    if (OidIsValid(oldrelid))
    {
        // Relation exists - handle based on IF NOT EXISTS flag
        if (!ctas->if_not_exists)
        {
            // Error if IF NOT EXISTS not specified
            ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_TABLE),
                     errmsg("relation \"%s\" already exists", into->rel->relname)));
        }

        // IF NOT EXISTS specified - validate extension membership for security
        ObjectAddressSet(address, RelationRelationId, oldrelid);
        checkMembershipInCurrentExtension(&address);

        // Issue notice and skip creation
        ereport(NOTICE,
                (errcode(ERRCODE_DUPLICATE_TABLE),
                 errmsg("relation \"%s\" already exists, skipping", into->rel->relname)));
        return true;
    }

    // Relation doesn't exist - creation can proceed
    return false;
}
```