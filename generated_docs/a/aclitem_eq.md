# aclitem_eq

## Location
[src/backend/utils/adt/acl.c:748-767](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L748-L767)

## Overview
A PostgreSQL system function that implements the equality operator for AclItem structures, comparing two access control list entries for exact equality.

## Definition
```c
Datum aclitem_eq(PG_FUNCTION_ARGS)
```

## Detailed Description
The aclitem_eq function is a PostgreSQL system function that implements the equality operator (=) for the aclitem data type. It performs a comprehensive comparison of two AclItem structures by checking all three key components: privileges (ai_privs), grantee (ai_grantee), and grantor (ai_grantor). The function returns true only when all three fields are identical between the two AclItem structures being compared.

This function is part of PostgreSQL's type system infrastructure, allowing AclItem values to be compared using the standard SQL equality operator. It uses PostgreSQL's function call convention with PG_FUNCTION_ARGS and returns a Datum value containing the boolean result.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS convention:
  - Argument 0: First AclItem structure to compare (accessed via PG_GETARG_ACLITEM_P(0))
  - Argument 1: Second AclItem structure to compare (accessed via PG_GETARG_ACLITEM_P(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ACLITEM_P (macro to extract AclItem arguments)
  - PG_RETURN_BOOL (macro to return boolean result)
  - AclItem (structure type)
- Called from (representative examples):
  - SQL equality operations on aclitem values
  - [Hash](../H/Hash.md) table lookups requiring equality checks

## Notes and Other Information
- This is a PostgreSQL system function accessible via SQL as the = operator for aclitem types
- Performs field-by-field comparison: ai_privs, ai_grantee, and ai_grantor must all match
- Returns true only for exact matches; no partial equality or privilege subset checking
- Used internally by PostgreSQL's type system for operations like DISTINCT, GROUP BY, and hash joins involving aclitem values
- The function signature follows PostgreSQL's V1 calling convention for system functions