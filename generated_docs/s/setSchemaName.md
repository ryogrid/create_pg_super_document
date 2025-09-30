# setSchemaName

## Location
[src/backend/parser/parse_utilcmd.c:3912-3931](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_utilcmd.c#L3912-L3931)

## Overview
Sets or validates the schema name in an element of a CREATE SCHEMA command, ensuring consistency between the context schema and statement schema.

## Definition
```c
static void setSchemaName(const char *context_schema, char **stmt_schema_name)
```

## Detailed Description
This static function manages schema name assignment and validation within CREATE SCHEMA statement elements. When a statement element doesn't specify a schema name, the function assigns the context schema name from the CREATE SCHEMA statement. If a schema name is already specified in the statement element, it validates that this name matches the context schema, raising an error if there's a mismatch.

The function ensures that all elements within a CREATE SCHEMA statement are consistently assigned to the intended schema, preventing conflicts between explicitly specified schema names and the schema being created.

## Parameters / Member Variables
- `context_schema`: The name of the schema being created, used as the default schema for statement elements
- `stmt_schema_name`: Pointer to the schema name field in the statement element; modified if NULL, validated if non-NULL

## Dependencies
- Functions called/Symbols referenced:
  - unconstify
  - strcmp
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
- Called from (representative examples):
  - [transformCreateSchemaStmtElements](../t/transformCreateSchemaStmtElements.md) (multiple calls for different statement types)

## Notes and Other Information
- This is a static helper function used exclusively within the parse_utilcmd.c file
- Uses unconstify to safely assign a const string to a non-const pointer when setting the schema name
- Generates ERRCODE_INVALID_SCHEMA_DEFINITION error when schema names don't match
- Critical for maintaining schema consistency across all elements in a CREATE SCHEMA statement
- The function modifies the stmt_schema_name parameter by reference when it's initially NULL

## Simplified Source

```c
static void
setSchemaName(const char *context_schema, char **stmt_schema_name)
{
    if (*stmt_schema_name == NULL) {
        // Set schema name from context if not specified
        *stmt_schema_name = unconstify(char *, context_schema);
    } else if (strcmp(context_schema, *stmt_schema_name) != 0) {
        // Validate that specified schema matches context schema
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_SCHEMA_DEFINITION),
                 errmsg("CREATE specifies a schema (%s) different from the one being created (%s)",
                        *stmt_schema_name, context_schema)));
    }
}
```