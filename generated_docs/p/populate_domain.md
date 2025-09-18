# populate_domain

## Location
src/backend/utils/adt/jsonfuncs.c: 3215 - 3248

## Overview
Populates a domain type value from JSON/JsonB input by converting the underlying base type and applying domain-specific constraints.

## Definition
```c
static Datum populate_domain(DomainIOData *io,
                            Oid typid,
                            const char *colname,
                            MemoryContext mcxt,
                            JsValue *jsv,
                            bool *isnull,
                            Node *escontext,
                            bool omit_quotes)
```

## Detailed Description
populate_domain handles the conversion of JSON/JsonB values into PostgreSQL domain types, which are user-defined types based on existing data types with additional constraints. The function operates in two main phases:

1. **Base Type Conversion**: Delegates to populate_record_field() to convert the JSON/JsonB value to the domain's underlying base type
2. **Domain Constraint Validation**: Applies all domain-specific constraints (CHECK constraints, NOT NULL constraints, etc.) using domain_check_safe()

The function follows PostgreSQL's type system hierarchy where domains add constraint layers on top of existing types. If any domain constraint fails, the function returns NULL and sets the appropriate error state through the soft error handling mechanism.

## Parameters / Member Variables
- `io`: DomainIOData structure containing base type information and domain constraint data
- `typid`: Target domain type OID
- `colname`: Column name for error reporting contexts
- `mcxt`: Memory context for result allocation
- `jsv`: Input JSON/JsonB value to be converted
- `isnull`: Pointer to NULL indicator flag (input/output)
- `escontext`: Error context for soft error handling
- `omit_quotes`: Whether to strip quotes from string values during base type conversion

## Dependencies
- Functions called/Symbols referenced:
  - populate_record_field
  - domain_check_safe
  - SOFT_ERROR_OCCURRED
- Called from (representative examples):
  - populate_record_field
  - JsObjectFree

## Notes and Other Information
- Returns NULL datum and sets *isnull = true when domain constraints fail
- Uses PointerGetDatum(NULL) as default value parameter when calling populate_record_field()
- Relies on assertion checking to ensure consistent error state handling
- The function is essentially a wrapper that adds domain constraint checking to base type population
- Domain constraint failures are handled gracefully through the soft error mechanism rather than throwing exceptions
- Supports all domain constraint types including CHECK constraints and NOT NULL constraints
- The base type conversion is performed through populate_record_field() which handles the appropriate delegation to scalar, composite, or other population functions
- Memory context management is delegated to the underlying population and constraint checking functions