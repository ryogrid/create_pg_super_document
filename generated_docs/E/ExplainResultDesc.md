# ExplainResultDesc

## Location
[src/backend/commands/explain.c:389-427](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L389-L427)

## Overview
ExplainResultDesc constructs the result tuple descriptor for EXPLAIN command output, determining the appropriate column type based on the specified format option.

## Definition

```c
TupleDesc
ExplainResultDesc(ExplainStmt *stmt)
```
## Detailed Description
ExplainResultDesc creates a tuple descriptor that defines the structure of the result set returned by an EXPLAIN command. The function examines the EXPLAIN statement's options to determine the output format and sets the appropriate data type for the single result column. It supports three main formats: TEXT (TEXTOID), XML (XMLOID), and JSON (JSONOID). YAML format is treated as TEXT since PostgreSQL doesn't have a native YAML type.

The function iterates through all format options in the statement (not breaking after the first one) to use the last specified format value, which matches the behavior in ExplainQuery. It then creates a single-column tuple descriptor with the column name 'QUERY PLAN' and the determined data type.

## Parameters / Member Variables
- `*stmt`: ExplainStmt containing the EXPLAIN statement with options that determine the output format
## Dependencies
- Functions called/Symbols referenced:
  - [ExplainStmt](ExplainStmt.md) (struct type)
  - [DefElem](../D/DefElem.md) (struct type)
  - [defGetString](../d/defGetString.md)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)
  - [TupleDescInitEntry](../T/TupleDescInitEntry.md)
- Called from (representative examples):
  - [ExplainQuery](ExplainQuery.md)
  - [UtilityTupleDescriptor](../U/UtilityTupleDescriptor.md)

## Notes and Other Information
- Always creates a single-column result with column name 'QUERY PLAN'
- Supports three data types: TEXTOID (default), XMLOID, and JSONOID
- Uses the last format option found, consistent with ExplainQuery's behavior
- YAML format is treated as TEXT type since PostgreSQL lacks native YAML support
- The tuple descriptor has typmod -1 and attndims 0 for the single column

## Simplified Source

```c
// Simplified version of ExplainResultDesc
TupleDesc
ExplainResultDesc(ExplainStmt *stmt)
{
    TupleDesc tupdesc;
    Oid result_type = TEXTOID;  // Default to TEXT format

    // Scan through statement options to find format specification
    foreach(lc, stmt->options) {
        DefElem *opt = (DefElem *) lfirst(lc);

        if (strcmp(opt->defname, "format") == 0) {
            char *format_str = defGetString(opt);

            // Determine result column type based on format
            if (strcmp(format_str, "xml") == 0)
                result_type = XMLOID;
            else if (strcmp(format_str, "json") == 0)
                result_type = JSONOID;
            else
                result_type = TEXTOID;  // Default for text/yaml
        }
    }

    // Create single-column tuple descriptor for EXPLAIN output
    tupdesc = CreateTemplateTupleDesc(1);
    TupleDescInitEntry(tupdesc, 1, "QUERY PLAN", result_type, -1, 0);

    return tupdesc;
}
```

Key simplifications made:
- Added descriptive comments explaining each major step
- Clarified variable purpose (result_type initialization)
- Consolidated format checking logic with cleaner structure
- Simplified comments to focus on core functionality
- Maintained all essential logic while improving readability