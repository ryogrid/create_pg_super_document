# domain_check

## Location
src/backend/utils/adt/domains.c: 346 - 354

## Overview
Public API function to check that a datum satisfies the constraints of a domain type.

## Definition


## Detailed Description
The  function provides a public interface for validating that a given value satisfies all constraints defined for a specific domain type. This function is a simple wrapper around  that provides error handling through traditional ereport mechanisms rather than soft error contexts.

The function is designed for use by PostgreSQL subsystems that need to validate domain values outside of the normal input/output functions. It allows callers to optionally pass cached state information () and specify a memory context for allocations, making it suitable for both one-off checks and repeated validation operations.

This function is commonly used by procedural language implementations (PL/Perl, PL/Python, PL/Tcl) and by the expanded record system when dealing with domain-typed fields.

## Parameters / Member Variables
- : The Datum value to be validated against domain constraints
- : Boolean flag indicating whether the value is null
- : OID of the domain type whose constraints should be applied
- : Pointer to optional cache storage (can be NULL for one-off calls)
- : Memory context for allocations (can be NULL to use CurrentMemoryContext)

## Dependencies
- Functions called/Symbols referenced:
  - domain_check_internal

- Called from (representative examples):
  - [expanded_record_set_fields](../e/expanded_record_set_fields.md) (src/backend/utils/adt/expandedrecord.c:1359)
  - [check_domain_for_new_field](../c/check_domain_for_new_field.md) (src/backend/utils/adt/expandedrecord.c:1561)
  - [check_domain_for_new_tuple](../c/check_domain_for_new_tuple.md) (src/backend/utils/adt/expandedrecord.c:1587)
  - [plperl_sv_to_datum](../p/plperl_sv_to_datum.md) (src/pl/plperl/plperl.c:1406)
  - [PLyObject_ToDomain](../P/PLyObject_ToDomain.md) (src/pl/plpython/plpy_typeio.c:1106)
  - pltcl_build_tuple_result (src/pl/tcl/pltcl.c:3252)

## Notes and Other Information
- This is a wrapper function that always uses traditional error reporting (ereport) rather than soft error contexts
- The  parameter allows for performance optimization by caching DomainIOData across multiple calls
- If  is NULL, the setup is repeated for each call, which is less efficient for multiple validations
- The function will throw an error if domain constraints are violated
- Commonly used by procedural languages and composite type handling code
- Uses  as the actual implementation workhorse