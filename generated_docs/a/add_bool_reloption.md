# add_bool_reloption

## Location
[src/backend/access/common/reloptions.c:849-864](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L849-L864)

## Overview
Adds a new boolean reloption to the global reloption registry, making it available for use across the PostgreSQL system.

## Definition


## Detailed Description
This public function creates and registers a new boolean reloption in the global reloption system. It combines the initialization of a boolean reloption structure through init_bool_reloption and its registration through add_reloption. This function is the main entry point for adding boolean-type reloptions that can be used by various relation types throughout PostgreSQL.

## Parameters / Member Variables
- : A bits32 value specifying the kinds of relations this option applies to (e.g., tables, indexes)
- : String name of the boolean reloption that users will specify
- : Optional description string for the reloption (can be NULL)
- : The default boolean value for this reloption when not explicitly set
- : The lock mode required when setting this reloption

## Dependencies
- Functions called/Symbols referenced:
  - [init_bool_reloption](../i/init_bool_reloption.md) (for boolean reloption initialization)
  - [add_reloption](add_reloption.md) (for global registration of the reloption)
- Called from (representative examples):
  - [create_reloptions_table](../c/create_reloptions_table.md) (in test modules)
  - Various extension and built-in code that needs to register boolean reloptions

## Notes and Other Information
- This is a public function, accessible from other files that include the appropriate headers
- The function follows the two-step pattern: initialize the specific type, then register globally
- Once registered, the reloption becomes available for use in CREATE and ALTER statements
- The function is commonly used during server startup or extension loading to register custom options