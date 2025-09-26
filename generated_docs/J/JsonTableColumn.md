# JsonTableColumn

## Location
[src/include/nodes/parsenodes.h:1851-1865](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L1851-L1865)

## Overview
JsonTableColumn represents the untransformed parse tree representation of a single column definition within a JSON_TABLE expression, specifying how JSON data should be extracted and formatted for a particular output column.

## Definition
```c
typedef struct JsonTableColumn
{
    NodeTag                 type;
    JsonTableColumnType     coltype;        /* column type */
    char                   *name;           /* column name */
    TypeName               *typeName;       /* column type name */
    JsonTablePathSpec      *pathspec;       /* JSON path specification */
    JsonFormat             *format;         /* JSON format clause, if specified */
    JsonWrapper             wrapper;        /* WRAPPER behavior for formatted columns */
    JsonQuotes              quotes;         /* omit or keep quotes on scalar strings? */
    List                   *columns;        /* nested columns */
    JsonBehavior           *on_empty;       /* ON EMPTY behavior */
    JsonBehavior           *on_error;       /* ON ERROR behavior */
    ParseLoc                location;       /* token location, or -1 if unknown */
} JsonTableColumn;
```

## Detailed Description
JsonTableColumn defines how a specific column should be extracted from JSON data in a JSON_TABLE operation. It supports various column types including simple value extraction, nested JSON objects, and formatted output. The structure contains comprehensive formatting options, error handling behaviors, and supports nested column hierarchies for complex JSON structures. Each column can have its own JSON path specification, data type, and behavioral controls for empty values and errors.

## Parameters / Member Variables
- `type`: Standard NodeTag identifying this as a JsonTableColumn node
- `coltype`: JsonTableColumnType enum specifying the kind of column (FOR ORDINALITY, PATH, etc.)
- `name`: String containing the output column name
- `typeName`: TypeName structure specifying the PostgreSQL data type for the column
- `pathspec`: JsonTablePathSpec defining the JSON path to extract data for this column
- `format`: JsonFormat specifying JSON formatting options for the output
- `wrapper`: JsonWrapper enum controlling how arrays/objects are wrapped in output
- `quotes`: JsonQuotes enum controlling quote handling for scalar string values
- `columns`: List of nested JsonTableColumn structures for hierarchical data
- `on_empty`: JsonBehavior defining what to do when the JSON path yields no data
- `on_error`: JsonBehavior defining what to do when errors occur during extraction
- `location`: ParseLoc for tracking the position in the source query

## Dependencies
- Functions called/Symbols referenced:
  - JsonTableColumnType (column type classification)
  - [TypeName](../T/TypeName.md) (PostgreSQL type system)
  - [JsonTablePathSpec](JsonTablePathSpec.md) (path specification)
  - [JsonFormat](JsonFormat.md) (formatting options)
  - JsonWrapper (wrapping behavior)
  - JsonQuotes (quote handling)
  - [JsonBehavior](JsonBehavior.md) (error/empty handling)
  - ParseLoc (location tracking)
- Called from (representative examples):
  - [transformJsonTableColumns](../t/transformJsonTableColumns.md) (column list processing)
  - [transformJsonTableColumn](../t/transformJsonTableColumn.md) (individual column transformation)
  - [transformJsonTableNestedColumns](../t/transformJsonTableNestedColumns.md) (nested column processing)
  - [CheckDuplicateColumnOrPathNames](../C/CheckDuplicateColumnOrPathNames.md) (validation)

## Notes and Other Information
- Supports nested column definitions for extracting data from complex JSON structures
- Each column can have independent error handling and empty value behavior
- The coltype field determines the specific behavior and required fields for the column
- Format and wrapper options provide fine-grained control over JSON output representation
- Part of the comprehensive SQL/JSON standard implementation in PostgreSQL
- Can represent both simple scalar columns and complex nested object/array columns