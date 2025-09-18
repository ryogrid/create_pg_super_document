# CreateDestReceiver

## Location
[src/backend/tcop/dest.c:113-168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/dest.c#L113-L168)

## Overview
CreateDestReceiver is a factory function that returns the appropriate DestReceiver function set based on the specified destination type for query output.

## Definition
DestReceiver *CreateDestReceiver(CommandDest dest)

## Detailed Description
This function serves as the central factory for creating destination receiver objects in PostgreSQL's output management system. It implements a comprehensive switch statement that maps CommandDest enumeration values to their corresponding DestReceiver implementations. The function handles all supported destination types, from simple client output to complex scenarios like tuplestore collection, SQL function returns, and replication streaming. Each destination type gets either a pre-configured static receiver or a dynamically created receiver with specific capabilities.

## Parameters / Member Variables
- dest: CommandDest enum value specifying the type of destination for query results (e.g., DestRemote for client output, DestNone for discarded output, DestTuplestore for internal collection)

## Dependencies
- Functions called/Symbols referenced:
  - [printtup_create_DR](../p/printtup_create_DR.md) (creates receivers for client output)
  - unconstify (removes const qualifier for static receivers)
  - [CreateTuplestoreDestReceiver](CreateTuplestoreDestReceiver.md) (creates tuplestore collectors)
  - [CreateIntoRelDestReceiver](CreateIntoRelDestReceiver.md) (creates table creation receivers)  
  - [CreateCopyDestReceiver](CreateCopyDestReceiver.md) (creates COPY command receivers)
  - [CreateSQLFunctionDestReceiver](CreateSQLFunctionDestReceiver.md) (creates SQL function return receivers)
  - [CreateTransientRelDestReceiver](CreateTransientRelDestReceiver.md) (creates temporary relation receivers)
  - [CreateTupleQueueDestReceiver](CreateTupleQueueDestReceiver.md) (creates tuple queue receivers)
  - [CreateExplainSerializeDestReceiver](CreateExplainSerializeDestReceiver.md) (creates EXPLAIN serialization receivers)
  - pg_unreachable (marks unreachable code)
- Called from (representative examples):
  - [exec_simple_query](../e/exec_simple_query.md) (for simple query execution)
  - [exec_execute_message](../e/exec_execute_message.md) (for prepared statement execution)
  - [SPI_execute_plan](../S/SPI_execute_plan.md) (for SPI query execution)
  - Various replication and backup functions

## Notes and Other Information
- Central factory function for all DestReceiver types in PostgreSQL
- Uses unconstify macro to safely cast away const from static receiver structs
- Includes comprehensive coverage of all CommandDest enum values
- Ends with pg_unreachable() indicating all cases should be handled
- Critical component in query output routing and management
- Static receivers are reused for efficiency while dynamic receivers are created per-use