# SPI_register_trigger_data

## Location
[src/backend/executor/spi.c:3364-3404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L3364-L3404)

## Overview
Registers transient relations from trigger data using the current SPI connection, making transition tables visible to queries run in the connection.

## Definition


## Detailed Description
This function is designed for use by procedural language (PL) implementations' trigger handlers. It extracts transition tables (NEW and OLD tables) from TriggerData and registers them as ephemeral named relations so they can be referenced in SQL queries executed within the trigger context. The function handles both tg_newtable and tg_oldtable if they exist, creating EphemeralNamedRelation structures for each and registering them using SPI_register_relation. Each relation is configured as a named tuplestore with metadata extracted from the trigger data.

## Parameters / Member Variables
- : Pointer to TriggerData structure containing trigger context information including transition tables (tg_newtable and tg_oldtable), trigger definition, and relation information.

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (for memory allocation)
  - [tuplestore_tuple_count](../t/tuplestore_tuple_count.md) (to get tuple count for metadata)
  - [SPI_register_relation](SPI_register_relation.md) (to register each ephemeral named relation)
- Called from (representative examples):
  - [plperl_trigger_handler](../p/plperl_trigger_handler.md) (Perl trigger handler)
  - [PLy_exec_trigger](../P/PLy_exec_trigger.md) (Python trigger handler)
  - [pltcl_trigger_handler](../p/pltcl_trigger_handler.md) (Tcl trigger handler)
  - [plsample_trigger_handler](../p/plsample_trigger_handler.md) (sample PL trigger handler)

## Notes and Other Information
- Returns SPI_ERROR_ARGUMENT if tdata is NULL
- Returns error codes from SPI_register_relation if registration fails
- Returns SPI_OK_TD_REGISTER on successful registration of all available tables
- Creates ENR_NAMED_TUPLESTORE type ephemeral relations
- Sets tupdesc to NULL as it's inferred from the tuplestore
- Uses trigger names from tdata->tg_trigger->tgnewtable and tdata->tg_trigger->tgoldtable
- Essential for making transition tables accessible in trigger-executed SQL
- Used by all major procedural language implementations in PostgreSQL