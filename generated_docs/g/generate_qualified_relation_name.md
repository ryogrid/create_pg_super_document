# generate_qualified_relation_name

## Location
[src/backend/utils/adt/ruleutils.c:12883-12926](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L12883-L12926)

## Overview
Computes the fully schema-qualified name to display for a relation specified by OID, always including the schema prefix regardless of search path visibility.

## Definition

```c
static char *
generate_qualified_relation_name(Oid relid)
```
## Detailed Description
This function is a simpler variant of generate_relation_name() that unconditionally generates a schema-qualified relation name. Unlike generate_relation_name(), it does not check for CTE name conflicts or search path visibility - it always includes the schema name. This is useful when you need to ensure the generated name is fully qualified and unambiguous, regardless of the current database context. The function performs system catalog lookups to retrieve both the relation name and its namespace, then combines them using proper SQL identifier quoting.

## Parameters / Member Variables
- `relid`: The OID of the relation to generate a qualified name for

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - NameStr
  - [get_namespace_name_or_temp](get_namespace_name_or_temp.md)
  - [quote_qualified_identifier](../q/quote_qualified_identifier.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - elog
- Called from (representative examples):
  - [pg_get_triggerdef_worker](../p/pg_get_triggerdef_worker.md)
  - [pg_get_indexdef_worker](../p/pg_get_indexdef_worker.md)
  - [pg_get_constraintdef_worker](../p/pg_get_constraintdef_worker.md)
  - [pg_get_serial_sequence](../p/pg_get_serial_sequence.md)
  - [make_ruledef](../m/make_ruledef.md)
  - [NameHashEntry](../N/NameHashEntry.md) structure usage

## Notes and Other Information
- This is a static function local to ruleutils.c
- Always generates fully qualified names (schema.relation) regardless of context
- Provides robust error handling for both relation and namespace lookup failures
- Used when unambiguous naming is required, such as in system catalog references or when exporting definitions
- The returned string is palloc'd and must be freed by the caller
- Complements generate_relation_name() by providing a simpler interface when qualification is always desired