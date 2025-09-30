# check_valid_internal_signature

## Location
[src/backend/parser/parse_coerce.c:2954-2977](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L2954-L2977)

## Overview
Validates whether a proposed function signature is safe according to PostgreSQL's INTERNAL type safety rules.

## Definition

```c
char *
check_valid_internal_signature(Oid ret_type,
							   const Oid *declared_arg_types,
							   int nargs)
```
## Detailed Description
This function enforces safety rules for functions that return the INTERNAL pseudotype. The INTERNAL type is used internally by PostgreSQL for passing opaque data structures between functions and is not meant to be directly manipulated by SQL code.

The safety rule is simple but critical: if a function returns INTERNAL type, it must also accept at least one INTERNAL type as input. This prevents creation of functions that could be called directly from SQL to produce INTERNAL values, which would be unsafe since SQL users shouldn't be able to create arbitrary INTERNAL values.

Functions that both accept and return INTERNAL types are typically system functions used for implementing complex features like aggregates, where INTERNAL values are passed between related functions (like aggregate transition functions) but never exposed to end users.

## Parameters / Member Variables
- : The return type OID of the function being validated
- : Array of declared argument type OIDs for the function
- : Number of function arguments

## Dependencies
- Functions called/Symbols referenced:
  - [pstrdup](../p/pstrdup.md)
  - INTERNALOID (constant)
- Called from (representative examples):
  - [ProcedureCreate](../P/ProcedureCreate.md)
  - [AggregateCreate](../A/AggregateCreate.md)

## Notes and Other Information
- Located in src/backend/parser/parse_coerce.c:2954-2977
- Returns NULL if the signature is safe, otherwise returns a palloc'd error message string
- Used during CREATE FUNCTION and CREATE AGGREGATE to validate INTERNAL type usage
- Error message is already translated for internationalization
- API is kept similar to check_valid_polymorphic_signature for consistency
- Critical for maintaining type safety and preventing unsafe SQL operations with INTERNAL types
- Part of PostgreSQL's defense against potential security vulnerabilities related to internal data structures

## Simplified Source

```c
char *
check_valid_internal_signature(Oid ret_type, const Oid *declared_arg_types, int nargs)
{
    // Only functions returning INTERNAL type need validation
    if (ret_type == INTERNALOID) {
        // Check if at least one argument is also INTERNAL type
        for (int i = 0; i < nargs; i++) {
            if (declared_arg_types[i] == ret_type)
                return NULL;  // Safe - has INTERNAL input
        }
        // Unsafe - returns INTERNAL but no INTERNAL inputs
        return pstrdup(_("A result of type internal requires at least one input of type internal."));
    }

    return NULL;  // Safe - not returning INTERNAL type
}
```