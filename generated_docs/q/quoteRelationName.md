# quoteRelationName

## Location
[src/backend/utils/adt/ri_triggers.c:1893-1909](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L1893-L1909)

## Overview
A utility function that safely quotes a fully qualified relation name (schema.table) into a character buffer for use in SQL queries and error messages.

## Definition

```c
static void
quoteRelationName(char *buffer, Relation rel)
```
## Detailed Description
This function constructs a properly quoted, fully qualified relation name by combining the namespace (schema) name and relation (table) name with appropriate quoting. It formats the output as "schema"."table" where both the schema and table names are individually quoted using the  function. The function is designed to handle relation names that may contain special characters or reserved keywords that require quoting in SQL contexts.

The function operates by:
1. Quoting the namespace name and placing it in the buffer
2. Advancing the buffer pointer to the end of the namespace name
3. Adding a period (.) separator
4. Quoting the relation name and appending it to the buffer

## Parameters / Member Variables
- : A character array that must be at least MAX_QUOTED_REL_NAME_LEN bytes long (includes room for null terminator) to store the fully quoted relation name
- : A Relation structure representing the database relation whose name is to be quoted

## Dependencies
- Functions called/Symbols referenced:
  - : Retrieves the namespace name for the relation
  - : Gets the namespace OID from the relation
  - : Quotes individual names (schema and table names) for safe SQL usage
  - : Gets the relation name from the relation structure

- Called from (representative examples):
  - : Used in primary key matching checks
  - : Used in referential integrity restriction operations
  - : Used in foreign key cascade delete operations
  - : Used in foreign key cascade update operations
  - : Used in referential integrity set operations
  - : Used in initial referential integrity checks
  - : Used in partition removal checks

## Notes and Other Information
- This is a static function within the ri_triggers.c file, indicating it's an internal utility for referential integrity operations
- The buffer size requirement (MAX_QUOTED_REL_NAME_LEN) ensures sufficient space for the longest possible quoted relation name
- The function assumes the input buffer is properly allocated and sized
- Used extensively throughout PostgreSQL's referential integrity trigger system for generating error messages and constructing SQL queries
- The quoting mechanism ensures that relation names containing special characters, spaces, or SQL keywords are properly escaped

## Simplified Source

```c
static void quoteRelationName(char *buffer, Relation rel) {
    // Quote the schema name first
    quoteOneName(buffer, get_namespace_name(RelationGetNamespace(rel)));

    // Move to end of schema name and add dot separator
    buffer += strlen(buffer);
    *buffer++ = '.';

    // Quote the table name
    quoteOneName(buffer, RelationGetRelationName(rel));
}
```