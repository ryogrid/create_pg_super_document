# equalPolicy

## Location
src/backend/utils/cache/relcache.c: 953 - 998

## Overview
Determines whether two RowSecurityPolicy structures are equivalent by comparing their command types, sublink flags, names, roles, and qualification expressions.

## Definition


## Detailed Description
This function performs a comprehensive comparison of two row security policy structures to determine equivalence. It compares all significant attributes of the policies including the command type they apply to, whether they contain sublinks, the policy names, the roles they apply to, and both the main qualification expression and the with-check qualification expression.

The function handles null pointer cases and performs deep comparison of array data for roles and Node structures for qualification expressions.

## Parameters / Member Variables
- : First RowSecurityPolicy structure to compare (may be NULL)
- : Second RowSecurityPolicy structure to compare (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [RowSecurityPolicy](../R/RowSecurityPolicy.md) (structure type)
  - ARR_DIMS (macro for array dimensions)
  - ARR_DATA_PTR (macro for array data pointer)
  - [equal](equal.md) (function for comparing Node structures)
- Called from (representative examples):
  - [equalRSDesc](equalRSDesc.md)

## Notes and Other Information
- Compares policy command types (polcmd) which determine what operations the policy applies to (SELECT, INSERT, UPDATE, DELETE)
- Checks hassublinks flag which indicates whether the policy expressions contain subqueries
- Performs string comparison on policy names using strcmp
- Compares role arrays by first checking dimensions then comparing individual OIDs
- Uses equal() function for deep comparison of qual and with_check_qual Node expressions
- Part of PostgreSQL's Row Level Security (RLS) system for fine-grained access control