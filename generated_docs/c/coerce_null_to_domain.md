# coerce_null_to_domain

## Location
src/backend/parser/parse_coerce.c: 1273 - 1313

## Overview
Creates a NULL constant value for a domain type, properly handling domain constraints by wrapping it in a CoerceToDomain node when necessary to enforce NOT NULL constraints at runtime.

## Definition
```c
Node *coerce_null_to_domain(Oid typid, int32 typmod, Oid collation,
                           int typlen, bool typbyval)
```

## Detailed Description
This function constructs a NULL constant that is properly typed for a domain. When the target type is a domain type (as opposed to a base type), it creates a NULL constant with the domain's base type characteristics and then wraps it in a CoerceToDomain node. This wrapper ensures that any domain constraints, particularly NOT NULL constraints, can be evaluated and enforced at runtime.

The function first determines the base type and typmod of the domain using `getBaseTypeAndTypmod`, then creates a NULL constant of that base type. If the requested type is indeed a domain (different from the base type), it applies domain coercion to ensure proper constraint checking.

## Parameters / Member Variables
- `typid`: OID of the target domain type
- `typmod`: Type modifier for the domain
- `collation`: Collation OID for the domain
- `typlen`: Length of the type (-1 for variable-length types)
- `typbyval`: Whether the type is passed by value (true) or reference (false)

## Dependencies
- Functions called/Symbols referenced:
  - getBaseTypeAndTypmod
  - makeConst
  - coerce_to_domain
  - COERCION_IMPLICIT (constant)
  - COERCE_IMPLICIT_CAST (constant)
- Called from (representative examples):
  - expand_insert_targetlist (src/backend/optimizer/prep/preptlist.c:473)
  - rewriteTargetListIU (src/backend/rewrite/rewriteHandler.c:1005)
  - rewriteValuesRTE (src/backend/rewrite/rewriteHandler.c:1557)
  - ReplaceVarsFromTargetList_callback (src/backend/rewrite/rewriteManip.c:1734)

## Notes and Other Information
- Essential for proper handling of domain types in INSERT operations and query rewriting
- The NULL constant is created with the domain's base type characteristics to prevent unnecessary length coercions
- Domain constraint checking (including NOT NULL constraints) is deferred to runtime through the CoerceToDomain wrapper
- Used primarily in query rewriting and optimization phases where NULL values need to be inserted for missing columns in domain-typed fields