# makeMultirangeTypeName

## Location
[src/backend/catalog/pg_type.c:950-982](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_type.c#L950-L982)

## Overview
makeMultirangeTypeName generates a unique multirange type name from a given range type name, following PostgreSQL's naming conventions for automatically created multirange types.

## Definition
char *makeMultirangeTypeName(const char *rangeTypeName, Oid typeNamespace)

## Detailed Description
This function constructs a multirange type name based on a range type name using PostgreSQL's standard naming conventions. It implements two naming strategies: if the range type name contains the substring "range", it replaces "range" with "multirange"; otherwise, it appends "_multirange" to the type name.

The function ensures the resulting name fits within PostgreSQL's NAMEDATALEN limit (typically 64 bytes) by clipping the name appropriately using multibyte-aware string functions. It also validates that the generated name doesn't conflict with existing types in the specified namespace, raising an error if a duplicate is found.

This function is part of PostgreSQL's multirange type system introduced to handle collections of non-overlapping ranges, providing automatic naming for the corresponding multirange type when a range type is created.

## Parameters / Member Variables
- `rangeTypeName`: The name of the range type for which to generate a multirange type name
- `typeNamespace`: The namespace (schema) OID where the multirange type will be created

## Dependencies
- Functions called/Symbols referenced:
  - [pnstrdup](../p/pnstrdup.md): Creates a null-terminated copy of a string with specified length
  - [psprintf](../p/psprintf.md): PostgreSQL's sprintf equivalent for formatted string creation
  - [pg_mbcliplen](../p/pg_mbcliplen.md): Multibyte-aware string clipping function
  - SearchSysCacheExists2: Checks if a type name already exists in the system catalog
  - [CStringGetDatum](../C/CStringGetDatum.md): Converts C string to PostgreSQL Datum
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md): Converts OID to PostgreSQL Datum
  - [pstrdup](../p/pstrdup.md): Creates a copy of a string
  - NAMEDATALEN: Constant defining maximum name length
  - ERRCODE_DUPLICATE_OBJECT: Error code for duplicate object conflicts
- Called from (representative examples):
  - [DefineRange](../D/DefineRange.md): When creating range types that need corresponding multirange types

## Notes and Other Information
The caller is responsible for freeing the returned string using pfree(). The function uses a sophisticated naming strategy that preserves readability by replacing "range" with "multirange" when possible, falling back to suffix addition when "range" is not found in the name. If a naming conflict occurs, it provides detailed error messages with hints about manually specifying multirange type names using the "multirange_type_name" attribute. The function respects PostgreSQL's multibyte character encoding by using pg_mbcliplen for proper string truncation.