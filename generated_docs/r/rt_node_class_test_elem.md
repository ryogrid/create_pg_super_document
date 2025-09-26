# rt_node_class_test_elem

## Location
[src/test/modules/test_radixtree/test_radixtree.c:66-70](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_radixtree/test_radixtree.c#L66-L70)

## Overview
A test structure that defines test parameters for different radix tree node classes, associating each node class with its name and the number of keys needed to grow nodes into that size class.

## Definition
```c
typedef struct rt_node_class_test_elem
{
    char       *class_name;
    int         nkeys;
} rt_node_class_test_elem;
```

## Detailed Description
The `rt_node_class_test_elem` structure is used in the PostgreSQL radix tree test module to define test cases for different node size classes. Each element in this structure represents a specific node class configuration used for testing the radix tree implementation. The structure is part of the test infrastructure that validates how radix tree nodes grow and adapt based on the number of keys they contain.

This structure is used to create an array of test configurations (`rt_node_class_tests`) that covers all major radix tree node classes: node-4, node-16-lo, node-16-hi, node-48, and node-256. Each test configuration specifies the minimum number of keys required to trigger the growth into that particular node class.

## Parameters / Member Variables
- `class_name`: A string identifier for the radix tree node class being tested (e.g., "node-4", "node-16-lo", "node-16-hi", "node-48", "node-256")
- `nkeys`: The number of keys required to grow nodes into this size class, used as a test parameter to verify proper node class transitions

## Dependencies
- Functions called/Symbols referenced: None (simple data structure)
- Called from (representative examples):
  - test_basic (uses this structure to parameterize radix tree tests)
  - test_radixtree (iterates through array of these structures)

## Notes and Other Information
- This structure is specifically designed for testing PostgreSQL's radix tree implementation
- It's used in conjunction with the `rt_node_class_tests` array which contains predefined test configurations for all supported node classes
- The structure helps ensure comprehensive testing coverage across different radix tree node sizes and growth patterns
- Located in the test module at `src/test/modules/test_radixtree/test_radixtree.c:66-70`
- The nkeys values in the test array are carefully chosen to trigger transitions between different node class implementations