# get_tablefunc

## Location
[src/backend/utils/adt/ruleutils.c:11921-11939](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L11921-L11939)

## Overview
Dispatches table function deparsing to the appropriate specialized handler based on the table function type (XMLTABLE or JSON_TABLE).

## Definition
```c
static void get_tablefunc(TableFunc *tf, deparse_context *context, bool showimplicit)
```

## Detailed Description
This function serves as a dispatcher for table function deparsing in PostgreSQL's rule system. It examines the table function type and delegates the actual deparsing work to the appropriate specialized function:

1. **XMLTABLE functions**: Dispatched to get_xmltable()
2. **JSON_TABLE functions**: Dispatched to get_json_table()

The function provides a unified interface for handling different types of table functions while keeping the implementation details separated into specialized handlers. This design supports extensibility for additional table function types that may be added in the future.

Currently, PostgreSQL supports two main types of table functions:
- XMLTABLE for extracting relational data from XML documents
- JSON_TABLE for extracting relational data from JSON documents

## Parameters / Member Variables
- `tf`: TableFunc structure containing the table function definition, including the function type that determines which handler to use
- `context`: deparse_context containing the output buffer and formatting state to be passed to the specialized handlers
- `showimplicit`: Boolean flag indicating whether to show implicit specifications, passed through to the specialized handlers

## Dependencies
- Functions called/Symbols referenced:
  - TFT_XMLTABLE (table function type constant)
  - TFT_JSON_TABLE (table function type constant)
  - [get_xmltable](get_xmltable.md) (XMLTABLE handler)
  - [get_json_table](get_json_table.md) (JSON_TABLE handler)
- Called from (representative examples):
  - [get_rule_expr](get_rule_expr.md)
  - [get_from_clause_item](get_from_clause_item.md)

## Notes and Other Information
- This is a static function used internally by the rule deparsing system
- The function uses a simple if-else chain for type dispatch, which is efficient given the small number of table function types
- The design pattern allows for easy extension when new table function types are added to PostgreSQL
- Both XMLTABLE and JSON_TABLE are relatively recent additions to SQL standard functionality
- The function maintains consistency in the deparse interface across different table function implementations
- Part of PostgreSQL's broader support for structured data processing (XML and JSON)