# SPI_getrelname

## Location
[src/backend/executor/spi.c:1326-1331](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L1326-L1331)

## Overview
Retrieves the relation name from a Relation structure as a dynamically allocated string.

## Definition
```c
char *SPI_getrelname(Relation rel)
```

## Detailed Description
SPI_getrelname is a simple utility function that extracts the relation name from a Relation structure and returns it as a newly allocated string. It serves as a convenient wrapper around the RelationGetRelationName macro, providing memory management by creating a duplicate of the relation name string that the caller owns and must eventually free.

This function is commonly used in procedural language implementations and trigger functions where code needs to access the name of a table or relation being operated on. The returned string is independent of the original Relation structure's lifecycle.

## Parameters / Member Variables
- `rel`: Relation structure from which to extract the name

## Dependencies
- Functions called/Symbols referenced:
  - [pstrdup](../p/pstrdup.md)
  - RelationGetRelationName
- Called from (representative examples):
  - [plperl_trigger_build_args](../p/plperl_trigger_build_args.md) (src/pl/plperl/plperl.c)
  - [PLy_trigger_build_args](../P/PLy_trigger_build_args.md) (src/pl/plpython/plpy_exec.c)
  - [pltcl_trigger_handler](../p/pltcl_trigger_handler.md) (src/pl/tcl/pltcl.c)
  - [plsample_trigger_handler](../p/plsample_trigger_handler.md) (src/test/modules/plsample/plsample.c)
  - [ttdummy](../t/ttdummy.md) (src/test/regress/regress.c)

## Notes and Other Information
- Returns a dynamically allocated string that must be freed by the caller
- Does not perform any validation on the input Relation pointer
- Commonly used in trigger functions and procedural language implementations
- Provides a safe way to obtain a relation name that persists beyond the Relation structure's lifetime
- No error handling - assumes valid Relation input
- Does not set any global SPI_result status