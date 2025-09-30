# stack_address_present_add_flags

## Location
[src/backend/catalog/dependency.c:2692-2741](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/dependency.c#L2692-L2741)

## Overview
Tests whether an object is present in an ObjectAddressStack (linked list) and if found, ORs additional flags into the object's associated data, implementing similar subobject relationship handling as its array counterpart.

## Definition

```c
static bool
stack_address_present_add_flags(const ObjectAddress *object,
								int flags,
								ObjectAddressStack *stack)
```
## Detailed Description
The  function provides stack-based equivalent functionality to , operating on a linked list structure (ObjectAddressStack) instead of an array. This function is used during dependency traversal to check for object presence while managing flags for complex object relationships.

Like its array counterpart, it handles three key scenarios:
1. **Exact match**: When both object and subobject IDs match, it ORs the provided flags into the stack entry's flags
2. **Subobject with whole object on stack**: When searching for a subobject but finding the whole object is already present, it returns true without flag propagation
3. **Whole object with subobject on stack**: When searching for a whole object but finding a subobject is present, it propagates flags to the subobject and marks it with DEPFLAG_SUBOBJECT

## Parameters / Member Variables
- : Pointer to the ObjectAddress to search for in the stack
- : Integer flags to OR into the found object's flag data
- : Pointer to the ObjectAddressStack (linked list) to search within and potentially modify

## Dependencies
- Functions called/Symbols referenced:
  - [ObjectAddressStack](../O/ObjectAddressStack.md) (struct type)
  - [ObjectAddress](../O/ObjectAddress.md) (struct type)
  - DEPFLAG_SUBOBJECT (flag constant)
- Called from (representative examples):
  - find_expr_references_context (src/backend/catalog/dependency.c:174)
  - [findDependentObjects](../f/findDependentObjects.md) (src/backend/catalog/dependency.c:469, 649)

## Notes and Other Information
- This is a static function, only accessible within the dependency.c module
- Traverses the entire linked list structure to handle all potential matches
- Implements the same subobject relationship logic as  but for stack-based data structures
- Used during recursive dependency analysis where a stack-based approach is more appropriate than array-based storage
- Critical for preventing circular dependencies and managing proper deletion order in PostgreSQL's dependency system

## Simplified Source

```c
static bool
stack_address_present_add_flags(const ObjectAddress *object,
                                int flags,
                                ObjectAddressStack *stack)
{
    bool result = false;

    // Traverse the linked list stack
    for (ObjectAddressStack *stackptr = stack; stackptr; stackptr = stackptr->next) {
        const ObjectAddress *thisobj = stackptr->object;

        // Check if class and object IDs match
        if (object->classId == thisobj->classId &&
            object->objectId == thisobj->objectId) {

            if (object->objectSubId == thisobj->objectSubId) {
                // Exact match: add flags to this stack entry
                stackptr->flags |= flags;
                result = true;
            }
            else if (thisobj->objectSubId == 0) {
                // Searching for subobject, but whole object is on stack
                // Skip further processing without flag propagation
                result = true;
            }
            else if (object->objectSubId == 0) {
                // Searching for whole object, but subobject is on stack
                // Propagate flags to subobject and mark it
                if (flags) {
                    stackptr->flags |= (flags | DEPFLAG_SUBOBJECT);
                }
            }
        }
    }

    return result;
}
```