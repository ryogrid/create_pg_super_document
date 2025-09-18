# DomainIOData

## Location
[src/backend/utils/adt/jsonfuncs.c:189-195](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L189-L195)

## Overview
DomainIOData is a structure used to cache state information across multiple calls when handling PostgreSQL domain types, including constraint validation and base type I/O operations.

## Definition


## Detailed Description
DomainIOData serves as a comprehensive cache structure for domain type processing in PostgreSQL. It maintains all necessary information for efficiently handling domain types including the base type's I/O functions, constraint validation references, and execution contexts. This caching mechanism significantly improves performance by avoiding repeated lookups of type information and constraint definitions during domain value processing operations.

## Parameters / Member Variables
- : OID of the domain type being processed
- : OID of the base type's input function for value conversion
- : OID parameter for the base type's I/O function
- : Type modifier for the base type
- : FmgrInfo structure containing cached function manager information for the base type's input function
- : Reference to cached list of domain constraint items that need to be validated
- : Expression context used for evaluating CHECK constraints defined on the domain
- : Memory context in which this cache structure is allocated

## Dependencies
- Functions called/Symbols referenced:
  - [DomainConstraintRef](DomainConstraintRef.md)
- Called from (representative examples):
  - [domain_state_setup](../d/domain_state_setup.md)
  - [domain_check_input](../d/domain_check_input.md)
  - [domain_in](../d/domain_in.md)
  - [domain_recv](../d/domain_recv.md)
  - domain_check_internal
  - [ColumnIOData](../C/ColumnIOData.md)
  - JsObjectFree
  - [populate_domain](../p/populate_domain.md)

## Notes and Other Information
- Defined in src/backend/utils/adt/domains.c at lines 50-64
- Central to PostgreSQL's domain type implementation, providing efficient caching for repeated operations
- Supports both input/output operations and constraint validation for domain types
- The structure maintains memory context information for proper memory management
- Used extensively in both core domain functions and JSON processing operations
- Essential for performance optimization when processing domain types with complex constraints