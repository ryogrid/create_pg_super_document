# TriggerTransition

## Location
src/include/nodes/parsenodes.h: 1737 - 1743

## Overview
A structure representing transition row or table naming clauses in PostgreSQL trigger definitions, used to specify OLD/NEW transition table references.

## Definition


## Detailed Description
TriggerTransition represents the syntactic structure for naming transition rows or tables in trigger definitions. This allows triggers to reference the OLD and NEW data sets using user-defined names in trigger functions. Initially, only transition tables are supported in the syntax and only for AFTER triggers, though the parser accepts other permutations to provide meaningful error messages from C code. This structure captures the essential information needed to establish the relationship between user-defined names and the underlying transition data.

## Parameters / Member Variables
- : Standard NodeTag for PostgreSQL node identification
- : User-defined name for the transition table or row reference
- : Boolean flag indicating whether this references NEW data (true) or OLD data (false)
- : Boolean flag indicating whether this is a table reference (true) or row reference (false)

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)
- Called from (representative examples):
  - CreateTriggerFiringOn

## Notes and Other Information
- Currently only transition tables are supported in the actual implementation, specifically for AFTER triggers
- The parser accepts other permutations (like transition rows or BEFORE triggers) to provide better error messaging
- This structure is part of the parse tree representation and helps bridge user syntax with internal trigger mechanisms
- Located in src/include/nodes/parsenodes.h at lines 1737-1743