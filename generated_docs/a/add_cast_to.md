# add_cast_to

## Location
[src/backend/utils/adt/ruleutils.c:13149-13179](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L13149-L13179)

## Overview
Adds a type cast specification to a StringInfo buffer using fully-qualified type names to avoid truncation issues.

## Definition
```c
static void add_cast_to(StringInfo buf, Oid typid)
```

## Detailed Description
This function appends a cast specification to the provided buffer in the format `::schema.typename`. It deliberately avoids using the `format_type_be()` function and instead constructs the type name manually to prevent corner cases where SQL-standard syntax could result in undesirable data truncation.

The function ensures that the cast will have a default (-1) target typmod by spelling out the type name explicitly. This is particularly important for types like CHARACTER, BIT, and other types where specifying the type using standard SQL syntax might cause unexpected behavior.

The generated cast uses fully-qualified type names (schema.typename) to ensure reliability regardless of the current search path settings.

## Parameters / Member Variables
- `buf`: StringInfo buffer to append the cast specification to
- `typid`: OID of the type to generate a cast specification for

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup for type information)
  - [get_namespace_name_or_temp](../g/get_namespace_name_or_temp.md) (namespace name resolution)
  - [quote_identifier](../q/quote_identifier.md) (identifier quoting for both schema and type names)
  - [appendStringInfo](appendStringInfo.md) (string buffer operations)
- Called from (representative examples):
  - [generate_operator_clause](../g/generate_operator_clause.md) (used when operand types need casting in operator expressions)

## Notes and Other Information
- Intentionally avoids format_type_be() to prevent data truncation issues
- Always uses fully-qualified type names (schema.typename) for reliability
- Ensures default typmod (-1) by explicit type name construction
- Critical for types like CHARACTER and BIT where SQL-standard syntax can cause problems
- Part of the internal SQL generation system where precision is essential