# generate_qualified_relation_name

## Location
src/backend/utils/adt/ruleutils.c: 12883 - 12926

## Overview
Computes the fully schema-qualified name to display for a relation specified by OID, always including the schema prefix regardless of search path visibility.

## Definition


## Detailed Description
This function is a simpler variant of generate_relation_name() that unconditionally generates a schema-qualified relation name. Unlike generate_relation_name(), it does not check for CTE name conflicts or search path visibility - it always includes the schema name. This is useful when you need to ensure the generated name is fully qualified and unambiguous, regardless of the current database context. The function performs system catalog lookups to retrieve both the relation name and its namespace, then combines them using proper SQL identifier quoting.

## Parameters / Member Variables
- `relid`: The OID of the relation to generate a qualified name for

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - HeapTupleIsValid
  - GETSTRUCT
  - NameStr
  - get_namespace_name_or_temp
  - quote_qualified_identifier
  - ReleaseSysCache
  - elog
- Called from (representative examples):
  - pg_get_triggerdef_worker
  - pg_get_indexdef_worker
  - pg_get_constraintdef_worker
  - pg_get_serial_sequence
  - make_ruledef
  - NameHashEntry structure usage

## Notes and Other Information
- This is a static function local to ruleutils.c
- Always generates fully qualified names (schema.relation) regardless of context
- Provides robust error handling for both relation and namespace lookup failures
- Used when unambiguous naming is required, such as in system catalog references or when exporting definitions
- The returned string is palloc'd and must be freed by the caller
- Complements generate_relation_name() by providing a simpler interface when qualification is always desired