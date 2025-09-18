# stringToQualifiedNameList

## Location
src/backend/utils/adt/regproc.c: 1797 - 1842

## Overview
Parses a C string into a qualified name list by splitting on dots and converting each component into PostgreSQL String nodes.

## Definition
```c
List *stringToQualifiedNameList(const char *string, Node *escontext)
```

## Detailed Description
The `stringToQualifiedNameList` function takes a C string containing a potentially qualified name (like "schema.table" or "database.schema.table") and converts it into a PostgreSQL List of String nodes. The function splits the input string on dot (`.`) separators and creates String nodes for each component, which is the standard format used throughout PostgreSQL for representing qualified object names.

The function includes comprehensive error handling through the escontext parameter, allowing callers to receive error information rather than having exceptions thrown. This is particularly useful in contexts where soft error handling is preferred.

## Parameters / Member Variables
- `string`: The input C string containing the qualified name to be parsed
- `escontext`: Error context node for soft error handling; if provided as ErrorSaveContext, errors are reported rather than thrown

## Dependencies
- Functions called/Symbols referenced:
  - SplitIdentifierString
  - ereturn
  - [makeString](../m/makeString.md)
  - [list_free](../l/list_free.md)
- Called from (representative examples):
  - [regprocin](../r/regprocin.md)
  - [regoperin](../r/regoperin.md)
  - [regclassin](../r/regclassin.md)
  - [regcollationin](../r/regcollationin.md)
  - [regconfigin](../r/regconfigin.md)
  - [regdictionaryin](../r/regdictionaryin.md)
  - [regrolein](../r/regrolein.md)
  - [regnamespacein](../r/regnamespacein.md)
  - [parseNameAndArgTypes](../p/parseNameAndArgTypes.md)
  - [thesaurus_init](../t/thesaurus_init.md)
  - [tsvector_update_trigger](../t/tsvector_update_trigger.md)
  - [getTSCurrentConfig](../g/getTSCurrentConfig.md)
  - [RelationNameGetTupleDesc](../R/RelationNameGetTupleDesc.md)

## Notes and Other Information
- Creates a modifiable copy of the input string using pstrdup() to avoid modifying the original
- Returns NIL for both parsing errors and empty input (empty input is considered an error)
- Performs memory cleanup by freeing both the duplicated raw string and the intermediate namelist
- Used extensively throughout PostgreSQL for parsing qualified names in registry type functions
- The function is critical for converting textual representations of object names into the internal List format expected by PostgreSQL's namespace resolution functions
- Located in src/backend/utils/adt/regproc.c with related registry type processing functions