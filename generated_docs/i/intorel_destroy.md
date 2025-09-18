# intorel_destroy

## Location
src/backend/commands/createas.c: 627 - 630

## Overview
intorel_destroy deallocates the memory used by a DR_intorel destination receiver object when it is no longer needed.

## Definition
static void intorel_destroy(DestReceiver *self)

## Detailed Description
This function serves as the destruction callback for DR_intorel destination receivers, responsible for final cleanup and memory deallocation. It simply frees the memory allocated for the DestReceiver structure, which was initially allocated in CreateIntoRelDestReceiver using palloc0. This function is called after intorel_shutdown has completed all operational cleanup, ensuring that only memory deallocation remains to be done.

## Parameters / Member Variables
- : The DestReceiver object to be deallocated, originally allocated as a DR_intorel structure

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [CreateIntoRelDestReceiver](../C/CreateIntoRelDestReceiver.md) (sets as callback)
  - DestReceiver cleanup sequences

## Notes and Other Information
This is the final step in the DR_intorel lifecycle, called only after all tuple processing and shutdown operations have completed. The function assumes that intorel_shutdown has already been called to handle operational cleanup like closing relations and freeing bulk insertion state. The simplicity of this function reflects PostgreSQL's clean separation between operational cleanup (shutdown) and memory management (destroy) phases in the DestReceiver interface.