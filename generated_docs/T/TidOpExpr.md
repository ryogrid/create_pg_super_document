# TidOpExpr

## Location
[src/backend/executor/nodeTidrangescan.c:45-50](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeTidrangescan.c#L45-L50)

## Overview
TidOpExpr represents an upper or lower range bound for TID (tuple identifier) range scans in PostgreSQL's executor, encapsulating the expression state and boundary conditions for efficient row scanning by physical location.

## Definition


## Detailed Description
TidOpExpr is a specialized data structure used in PostgreSQL's TID range scan executor node (nodeTidrangescan.c) to represent comparison operations that define range boundaries for scanning rows by their tuple identifiers (CTIDs). This structure is essential for optimizing queries that filter on CTID values using range operators like <, <=, >, >=.

The structure works in conjunction with the TidExprType enumeration (TIDEXPR_UPPER_BOUND, TIDEXPR_LOWER_BOUND) to specify whether the expression represents an upper or lower boundary of a scan range. This allows the executor to efficiently scan only the relevant portions of a table based on physical row locations.

## Parameters / Member Variables
- : A TidExprType value (TIDEXPR_UPPER_BOUND or TIDEXPR_LOWER_BOUND) indicating whether this expression represents an upper or lower range boundary
- : An ExprState pointer containing the compiled expression state for evaluating a subexpression that yields TID values
- : A boolean flag indicating whether the range boundary is inclusive (<=, >=) or exclusive (<, >)

## Dependencies
- Functions called/Symbols referenced:
  - TidExprType (enum with TIDEXPR_UPPER_BOUND, TIDEXPR_LOWER_BOUND values)
  - ExprState (PostgreSQL expression state structure)
- Called from (representative examples):
  - [MakeTidOpExpr](../M/MakeTidOpExpr.md) (creates TidOpExpr instances from OpExpr nodes)
  - [TidExprListCreate](TidExprListCreate.md) (builds lists of TidOpExpr for range scanning)
  - [TidRangeEval](TidRangeEval.md) (evaluates TidOpExpr during scan execution)

## Notes and Other Information
- [TidOpExpr](TidOpExpr.md) is specifically designed for CTID-based range scans and is not used for general expression evaluation
- The structure is allocated using palloc() and is part of the executor's memory context
- The inclusive field is currently initialized to false in MakeTidOpExpr, suggesting that inclusive boundary support may be limited or under development
- This structure is closely tied to the TID range scan optimization, which allows PostgreSQL to efficiently scan table pages based on physical tuple locations rather than sequential scanning