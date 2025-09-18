# intorel_receive

## Location
[src/backend/commands/createas.c:576-606](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/createas.c#L576-L606)

## Overview
intorel_receive handles the insertion of individual tuples into the target relation during CREATE TABLE AS and CREATE MATERIALIZED VIEW operations.

## Definition
static bool intorel_receive(TupleTableSlot *slot, DestReceiver *self)

## Detailed Description
This function serves as the tuple receiving callback for DR_intorel destination receivers. It is called for each tuple produced by the query execution and inserts it into the target relation using the table access method interface. The function respects the skipData option from the IntoClause - if WITH NO DATA was specified, no actual insertion occurs. The implementation notes that the input slot might not match the exact type of the target relation, but table_tuple_insert() handles this conversion, trading some efficiency for flexibility.

## Parameters / Member Variables
- : TupleTableSlot containing the tuple data to be inserted into the target relation
- : The DestReceiver object cast to DR_intorel containing the target relation and insertion state

## Dependencies
- Functions called/Symbols referenced:
  - table_tuple_insert
- Called from (representative examples):
  - [CreateIntoRelDestReceiver](../C/CreateIntoRelDestReceiver.md) (sets as callback)
  - Executor tuple processing pipeline

## Notes and Other Information
The function always returns true, indicating successful processing. Since the target relation is newly created, there are no indexes to maintain during insertion. The function uses the bulk insertion state, command ID, and table insertion options that were set up during intorel_startup. The design prioritizes simplicity and compatibility over optimal performance, accepting that slot type mismatches may cause slight inefficiencies rather than performing expensive type conversions.