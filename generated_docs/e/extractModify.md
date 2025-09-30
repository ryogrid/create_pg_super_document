# extractModify

## Location
[src/backend/commands/aggregatecmds.c:478-493](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/aggregatecmds.c#L478-L493)

## Overview
extractModify is a utility function that converts string representations of finalfunc_modify and mfinalfunc_modify parameters into their corresponding catalog enumeration values for aggregate function definitions.

## Definition

```c
static char
extractModify(DefElem *defel)
```
## Detailed Description
extractModify parses the string value from a DefElem parameter node representing finalfunc_modify or mfinalfunc_modify clauses in CREATE AGGREGATE statements. It converts the human-readable string values ("read_only", "shareable", "read_write") into their corresponding internal catalog representation constants (AGGMODIFY_READ_ONLY, AGGMODIFY_SHAREABLE, AGGMODIFY_READ_WRITE). These modify flags control how the aggregate's final function can access and modify the transition state, which is important for optimization and parallel execution safety. The function provides strict validation and reports a syntax error for any invalid values.

## Parameters / Member Variables
- : DefElem node containing the string value of the finalfunc_modify or mfinalfunc_modify parameter to be converted

## Dependencies
- Functions called/Symbols referenced:
  - [defGetString](../d/defGetString.md)
  - AGGMODIFY_READ_ONLY
  - AGGMODIFY_SHAREABLE  
  - AGGMODIFY_READ_WRITE
- Called from (representative examples):
  - [DefineAggregate](../D/DefineAggregate.md)

## Notes and Other Information
The modify flags control how PostgreSQL optimizes aggregate execution and whether the aggregate can be parallelized. READ_ONLY indicates the final function only reads the transition state, SHAREABLE allows sharing of transition state between parallel workers, and READ_WRITE indicates the final function may modify the transition state. This function is specifically used during aggregate definition parsing to validate and convert user-specified modify behavior into internal representation.

## Simplified Source

```c
static char
extractModify(DefElem *defel)
{
    // Get string value from DefElem
    char *val = defGetString(defel);

    // Convert string to catalog representation
    if (strcmp(val, "read_only") == 0)
        return AGGMODIFY_READ_ONLY;
    if (strcmp(val, "shareable") == 0)
        return AGGMODIFY_SHAREABLE;
    if (strcmp(val, "read_write") == 0)
        return AGGMODIFY_READ_WRITE;

    // Invalid value - report error
    ereport(ERROR,
            "parameter \"%s\" must be READ_only, shareable, or read_write",
            defel->defname);

    return 0;  // Keep compiler quiet
}
```