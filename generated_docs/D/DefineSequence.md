# DefineSequence

## Location
[src/backend/commands/sequence.c:121-261](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/sequence.c#L121-L261)

## Overview
DefineSequence creates a new sequence relation in PostgreSQL, handling all aspects of sequence creation including relation setup, initial data population, and catalog registration.

## Definition

```c
enumber for the sequence;
```
## Detailed Description
DefineSequence is the main function responsible for creating PostgreSQL sequences. It performs comprehensive sequence creation by:

1. **Duplicate Check**: If  is specified, checks for existing sequences with the same name
2. **Parameter Validation**: Calls  to validate and set sequence options (start, increment, min, max, cache, cycle)
3. **Relation Creation**: Creates the underlying relation structure with three columns:
   -  (INT8): The last generated sequence value
   -  (INT8): Log counter for WAL optimization
   -  (BOOL): Whether the sequence has been used
4. **Data Initialization**: Populates the sequence with initial data using 
5. **Ownership Processing**: Handles  clauses if specified
6. **Catalog Registration**: Inserts sequence metadata into  system catalog

The function ensures atomicity and proper locking throughout the creation process.

## Parameters / Member Variables
- : ParseState for query parsing context and error reporting
- : CreateSeqStmt containing sequence definition including name, options, ownership, and if_not_exists flag

## Dependencies
- Functions called/Symbols referenced:
  - [RangeVarGetAndCheckCreationNamespace](../R/RangeVarGetAndCheckCreationNamespace.md)
  - [checkMembershipInCurrentExtension](../c/checkMembershipInCurrentExtension.md)  
  - [init_params](../i/init_params.md)
  - makeColumnDef
  - [DefineRelation](DefineRelation.md)
  - [sequence_open](../s/sequence_open.md)/sequence_close
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [fill_seq_with_data](../f/fill_seq_with_data.md)
  - [process_owned_by](../p/process_owned_by.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (src/backend/tcop/utility.c:1667)

## Notes and Other Information
- Sequences are implemented as special relations with RELKIND_SEQUENCE
- The function handles both regular sequences and identity column sequences
- Extension membership is validated for CREATE IF NOT EXISTS operations for security
- The sequence relation uses AccessExclusiveLock during creation
- Initial sequence data is stored both in the relation and in pg_sequence catalog
- Supports all standard sequence options: START, INCREMENT, MINVALUE, MAXVALUE, CACHE, CYCLE, OWNED BY