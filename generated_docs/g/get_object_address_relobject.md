# get_object_address_relobject

## Location
[src/backend/catalog/objectaddress.c:1415-1493](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L1415-L1493)

## Overview
Finds the ObjectAddress for objects that are attached to a relation, such as rules, triggers, constraints, and policies.

## Definition

```c
struct return value. */
	attnum = get_attnum(reloid, attname);
```
## Detailed Description
The  function handles object address resolution for database objects that are dependent on or attached to relations. These include rules, triggers, table constraints, and row-level security policies. The function takes a qualified name where the last component is the dependent object name and the preceding components form the relation name.

The function first extracts the dependent object name from the last element of the object list, then constructs a relation name from the remaining elements. It opens the relation with AccessShareLock (not the lockmode from the caller, since that applies to the target object, not its parent relation). 

After opening the relation, the function switches on the object type and calls the appropriate lookup function (get_rewrite_oid for rules, get_trigger_oid for triggers, etc.). Each lookup function searches for the named object within the context of the specified relation.

The function includes careful resource management - if the target object is not found, it closes the relation to prevent resource leaks before returning an invalid ObjectAddress. If the object is found successfully, the opened relation is returned to the caller through the  parameter.

## Parameters / Member Variables
- : The type of relation-dependent object (OBJECT_RULE, OBJECT_TRIGGER, OBJECT_TABCONSTRAINT, or OBJECT_POLICY)
- : List of name components where the last element is the object name and preceding elements form the relation name
- : Output parameter that receives the opened relation (or NULL if object not found)
- : If true, return invalid ObjectAddress instead of throwing error when object not found

## Dependencies
- Functions called/Symbols referenced:
  - llast (to extract dependent object name)
  - [list_copy_head](../l/list_copy_head.md) (to extract relation name components)
  - table_openrv_extended (to open the relation)
  - [makeRangeVarFromNameList](../m/makeRangeVarFromNameList.md) (to convert name list to RangeVar)
  - [get_rewrite_oid](get_rewrite_oid.md) (for rules)
  - [get_trigger_oid](get_trigger_oid.md) (for triggers)
  - [get_relation_constraint_oid](get_relation_constraint_oid.md) (for table constraints)
  - [get_relation_policy_oid](get_relation_policy_oid.md) (for policies)
  - table_close (for cleanup on failure)
- Called from (representative examples):
  - [get_object_address](get_object_address.md)

## Notes and Other Information
This function is marked static and serves as a specialized helper within the objectaddress.c module. It uses AccessShareLock for the parent relation regardless of the lockmode requested for the target object, reflecting the principle that the lock applies to the object itself, not its container relation. The function handles the common pattern of relation-dependent objects where the object name must be qualified with its parent relation name. Proper resource management ensures that relations are not leaked when objects are not found.