# SchemaQuery

## Location
[src/bin/psql/tab-complete.c:122-204](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L122-L204)

## Overview
SchemaQuery is a struct used in PostgreSQL's psql tab completion system to define custom-built queries for obtaining possibly-schema-qualified names of database objects, providing a reusable template for constructing complex completion queries.

## Definition
```c
typedef struct SchemaQuery
{
    int         min_server_version;
    const char *catname;
    const char *selcondition;
    const char *viscondition;
    const char *namespace;
    const char *result;
    bool        use_distinct;
    const char *const *keywords;
    const char *refname;
    const char *refviscondition;
    const char *refnamespace;
} SchemaQuery;
```

## Detailed Description
The SchemaQuery structure is a comprehensive framework for building database object completion queries in psql. It addresses the complexity of handling schema-qualified object names while supporting visibility rules, version compatibility, and reference objects. The structure allows for flexible query construction by providing various components that are assembled with common boilerplate code in the _complete_from_query() function.

The system supports both simple object completion (like table names) and more complex scenarios where completion depends on related objects (such as completing column names for a specific table). It handles schema qualification automatically, applying appropriate visibility conditions and namespace joins when needed.

Like VersionedQuery, SchemaQuery supports server version-dependent variations through arrays of structures, allowing different query strategies for different PostgreSQL versions.

## Parameters / Member Variables
- `min_server_version`: Minimum PostgreSQL server version for this query structure (0 if no version restriction); used for version-dependent query arrays
- `catname`: Names of catalog tables to query with aliases (e.g., "pg_catalog.pg_class c"); pg_namespace joins are added automatically when needed
- `selcondition`: WHERE clause condition for filtering candidate rows (e.g., "c.relkind = 'r'"); NULL if no additional filtering needed
- `viscondition`: Visibility condition for unqualified object names (e.g., "pg_catalog.pg_table_is_visible(c.oid)"); NULL if not applicable
- `namespace`: Field name to join with pg_namespace.oid for schema qualification (e.g., "c.relnamespace"); NULL to ignore schema parts
- `result`: Base object name expression to return as completion result (e.g., "c.relname")
- `use_distinct`: Boolean flag to add DISTINCT clause to eliminate duplicate results
- `keywords`: NULL-terminated array of additional literal strings/keywords to include in completion results; NULL if none
- `refname`: Field to match against completion_ref_object when using reference-based completion (e.g., "cr.relname"); NULL if not using references
- `refviscondition`: Visibility condition for reference objects when completion_ref_schema is not set; NULL if not needed
- `refnamespace`: Field to join with pg_namespace.oid for reference object schema qualification; NULL if not considering reference schemas

## Dependencies
- Functions called/Symbols referenced:
  - [_complete_from_query](../c/_complete_from_query.md) (primary function that processes SchemaQuery)
  - [complete_from_schema_query](../c/complete_from_schema_query.md) (wrapper for single SchemaQuery)
  - [complete_from_versioned_schema_query](../c/complete_from_versioned_schema_query.md) (wrapper for SchemaQuery arrays)
- Called from (representative examples):
  - Query_for_list_of_tables (static SchemaQuery definition)
  - Query_for_list_of_functions (versioned SchemaQuery array)
  - Query_for_list_of_attributes (SchemaQuery with reference object support)

## Notes and Other Information
- Extensively used throughout src/bin/psql/tab-complete.c for defining completion queries for various database objects (tables, functions, types, etc.)
- Supports complex scenarios like completing column names for a specific table or index names for a particular table through the reference object mechanism
- The query construction automatically handles schema qualification logic, visibility checks, and namespace joins
- Arrays of SchemaQuery structures must be terminated with an entry having catname = NULL
- The completion system uses global variables like completion_ref_object and completion_ref_schema to pass context information
- Provides a clean separation between query structure definition and the common query assembly/execution logic