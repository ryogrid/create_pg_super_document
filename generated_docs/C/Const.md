# Const

## Location
[src/include/nodes/primnodes.h:306-336](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L306-L336)

## Overview
The Const structure represents a constant literal value in PostgreSQL's expression tree, storing typed constant data that appears in SQL queries.

## Definition
```c
typedef struct Const
{
    pg_node_attr(custom_copy_equal, custom_read_write)
    
    Expr        xpr;
    Oid         consttype;
    int32       consttypmod pg_node_attr(query_jumble_ignore);
    Oid         constcollid pg_node_attr(query_jumble_ignore);
    int         constlen pg_node_attr(query_jumble_ignore);
    Datum       constvalue pg_node_attr(query_jumble_ignore);
    bool        constisnull pg_node_attr(query_jumble_ignore);
    bool        constbyval pg_node_attr(query_jumble_ignore);
    ParseLoc    location pg_node_attr(query_jumble_location);
} Const;
```

## Detailed Description
The Const structure encapsulates constant values within PostgreSQL's expression system. It stores literal values that appear in SQL queries, such as numbers, strings, dates, or NULL values. The structure includes comprehensive type information and handles both pass-by-value and pass-by-reference data types. For varlena (variable-length) data types, the implementation enforces that values must be in non-extended form (4-byte header, no compression or external references) to ensure self-containment and reliable equality comparisons. The structure supports PostgreSQL's type system with full type, typmod, and collation information.

## Parameters / Member Variables
- `xpr`: Base expression node structure containing common expression properties
- `consttype`: PostgreSQL type system OID identifying the constant's data type
- `consttypmod`: Type modifier value providing additional type-specific information
- `constcollid`: OID of the collation for this constant, or InvalidOid if no collation applies
- `constlen`: Type length of the constant's data type (from pg_type.typlen)
- `constvalue`: The actual constant value stored as a Datum
- `constisnull`: Boolean flag indicating whether the constant represents a NULL value
- `constbyval`: Boolean flag indicating whether this data type is passed by value (true) or by reference (false)
- `location`: Token location in the original query text, used for error reporting and query jumbling

## Dependencies
- Functions called/Symbols referenced:
  - ParseLoc
- Called from (representative examples):
  - Used throughout PostgreSQL's query parsing, planning, and execution systems
  - Referenced in constant folding, expression evaluation, and optimization contexts

## Notes and Other Information
- The structure uses custom copy/equal/read/write functions as indicated by pg_node_attr annotations
- Most fields are ignored during query jumbling except for consttype and location
- Enforces non-extended form for varlena types to ensure reliable equality comparisons
- Location information is specifically tracked for query jumbling to mark constants as parameters
- Critical for constant folding optimizations and literal value handling throughout the system