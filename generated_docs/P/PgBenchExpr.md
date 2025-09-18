# PgBenchExpr

## Location
src/bin/pgbench/pgbench.h: 106 - 106

## Overview
PgBenchExpr is a struct that represents expression nodes in the pgbench expression evaluation system, supporting constants, variables, and function calls.

## Definition


## Detailed Description
PgBenchExpr is the core data structure for representing expressions in pgbench scripts. It uses a discriminated union design where the  field determines which member of the union is active. This allows pgbench to handle three types of expressions: constant values (literals), variable references, and function calls. The struct is designed to support recursive expression evaluation, where function calls can contain nested expressions as arguments through the PgBenchExprLink structure.

## Parameters / Member Variables
- : Discriminator field of type PgBenchExprType that indicates which union member is active (ENODE_CONSTANT, ENODE_VARIABLE, or ENODE_FUNCTION)
- : PgBenchValue containing a literal constant value when etype is ENODE_CONSTANT
- : String pointer to the variable name when etype is ENODE_VARIABLE
- : PgBenchFunction enum value specifying which function to call when etype is ENODE_FUNCTION
- : Pointer to PgBenchExprLink list containing function arguments when etype is ENODE_FUNCTION

## Dependencies
- Functions called/Symbols referenced:
  - PgBenchExprType (enum)
  - PgBenchValue (struct)
  - [PgBenchFunction](PgBenchFunction.md) (enum)
  - [PgBenchExprLink](PgBenchExprLink.md) (struct)
- Called from (representative examples):
  - [evaluateExpr](../e/evaluateExpr.md)
  - [executeMetaCommand](../e/executeMetaCommand.md)
  - [Command](../C/Command.md) struct

## Notes and Other Information
- Forward declared at line 106 in pgbench.h, with full definition at lines 110-126
- Used extensively in pgbench's expression parsing and evaluation system
- The union design provides memory efficiency while maintaining type safety through the etype discriminator
- Part of a recursive data structure where function arguments can themselves be complex expressions