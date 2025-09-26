# get_json_constructor_options

## Location
[src/backend/utils/adt/ruleutils.c:11408-11437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L11408-L11437)

## Overview
A static helper function that appends JSON constructor options to a StringInfo buffer during rule deparsing operations.

## Definition
```c
static void get_json_constructor_options(JsonConstructorExpr *ctor, StringInfo buf)
```

## Detailed Description
This function is responsible for formatting and appending various JSON constructor options to a string buffer during the deparsing process of JSON constructor expressions. It handles different types of JSON constructor expressions (objects, arrays, aggregates) and applies appropriate null handling, uniqueness constraints, and returning clauses based on the constructor type and flags.

The function processes three main categories of options:
1. **Null handling options**: Determines whether to append "ABSENT ON NULL" for object constructors or "NULL ON NULL" for array constructors
2. **Uniqueness constraints**: Adds "WITH UNIQUE KEYS" when the unique flag is set
3. **RETURNING clauses**: Delegates to get_json_returning() for most constructor types (excluding JSON_PARSE and JSON_SCALAR)

## Parameters / Member Variables
- `ctor`: A pointer to the JsonConstructorExpr structure containing the constructor information and flags
- `buf`: A StringInfo buffer where the formatted options will be appended

## Dependencies
- Functions called/Symbols referenced:
  - [appendStringInfoString](../a/appendStringInfoString.md) (for buffer operations)
  - [get_json_returning](get_json_returning.md) (for handling RETURNING clauses)
- Constants referenced:
  - JSCTOR_JSON_OBJECT
  - JSCTOR_JSON_OBJECTAGG  
  - JSCTOR_JSON_ARRAY
  - JSCTOR_JSON_ARRAYAGG
  - JSCTOR_JSON_PARSE
  - JSCTOR_JSON_SCALAR
- Called from:
  - [get_json_constructor](get_json_constructor.md)
  - [get_json_agg_constructor](get_json_agg_constructor.md)

## Notes and Other Information
- This is a static function within ruleutils.c, used specifically for rule deparsing operations
- The function applies different null handling semantics based on constructor type: object constructors default to "ABSENT ON NULL" behavior while array constructors use "NULL ON NULL"
- JSON_PARSE and JSON_SCALAR constructors do not support RETURNING clauses and are excluded from that processing
- The function is part of PostgreSQL's SQL rule deparsing infrastructure, converting internal representations back to readable SQL text