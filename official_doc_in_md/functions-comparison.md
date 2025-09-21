9.2. Comparison Functions and Operators  
---  
[Prev](functions-logical.md "9.1. Logical Operators") | [Up](functions.md "Chapter 9. Functions and Operators")| Chapter 9. Functions and Operators| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](functions-math.md "9.3. Mathematical Functions and Operators")  
  
* * *

## 9.2. Comparison Functions and Operators #

The usual comparison operators are available, as shown in [Table 9.1](functions-comparison.md#FUNCTIONS-COMPARISON-OP-TABLE "Table 9.1. Comparison Operators"). 

**Table 9.1. Comparison Operators**

Operator| Description  
---|---  
_`datatype`_ `<` _`datatype`_ → `boolean` | Less than  
_`datatype`_ `>` _`datatype`_ → `boolean` | Greater than  
_`datatype`_ `<=` _`datatype`_ → `boolean` | Less than or equal to  
_`datatype`_ `>=` _`datatype`_ → `boolean` | Greater than or equal to  
_`datatype`_ `=` _`datatype`_ → `boolean` | Equal  
_`datatype`_ `<>` _`datatype`_ → `boolean` | Not equal  
_`datatype`_ `!=` _`datatype`_ → `boolean` | Not equal  
  
  


### Note

`<>` is the standard SQL notation for “not equal”. `!=` is an alias, which is converted to `<>` at a very early stage of parsing. Hence, it is not possible to implement `!=` and `<>` operators that do different things. 

These comparison operators are available for all built-in data types that have a natural ordering, including numeric, string, and date/time types. In addition, arrays, composite types, and ranges can be compared if their component data types are comparable. 

It is usually possible to compare values of related data types as well; for example `integer` `>` `bigint` will work. Some cases of this sort are implemented directly by “cross-type” comparison operators, but if no such operator is available, the parser will coerce the less-general type to the more-general type and apply the latter's comparison operator. 

As shown above, all comparison operators are binary operators that return values of type `boolean`. Thus, expressions like `1 < 2 < 3` are not valid (because there is no `<` operator to compare a Boolean value with `3`). Use the `BETWEEN` predicates shown below to perform range tests. 

There are also some comparison predicates, as shown in [Table 9.2](functions-comparison.md#FUNCTIONS-COMPARISON-PRED-TABLE "Table 9.2. Comparison Predicates"). These behave much like operators, but have special syntax mandated by the SQL standard. 

**Table 9.2. Comparison Predicates**

Predicate  Description  Example(s)   
---  
_`datatype`_ `BETWEEN` _`datatype`_ `AND` _`datatype`_ → `boolean` Between (inclusive of the range endpoints).  `2 BETWEEN 1 AND 3` → `t` `2 BETWEEN 3 AND 1` → `f`  
_`datatype`_ `NOT BETWEEN` _`datatype`_ `AND` _`datatype`_ → `boolean` Not between (the negation of `BETWEEN`).  `2 NOT BETWEEN 1 AND 3` → `f`  
_`datatype`_ `BETWEEN SYMMETRIC` _`datatype`_ `AND` _`datatype`_ → `boolean` Between, after sorting the two endpoint values.  `2 BETWEEN SYMMETRIC 3 AND 1` → `t`  
_`datatype`_ `NOT BETWEEN SYMMETRIC` _`datatype`_ `AND` _`datatype`_ → `boolean` Not between, after sorting the two endpoint values.  `2 NOT BETWEEN SYMMETRIC 3 AND 1` → `f`  
_`datatype`_ `IS DISTINCT FROM` _`datatype`_ → `boolean` Not equal, treating null as a comparable value.  `1 IS DISTINCT FROM NULL` → `t` (rather than `NULL`)  `NULL IS DISTINCT FROM NULL` → `f` (rather than `NULL`)   
_`datatype`_ `IS NOT DISTINCT FROM` _`datatype`_ → `boolean` Equal, treating null as a comparable value.  `1 IS NOT DISTINCT FROM NULL` → `f` (rather than `NULL`)  `NULL IS NOT DISTINCT FROM NULL` → `t` (rather than `NULL`)   
_`datatype`_ `IS NULL` → `boolean` Test whether value is null.  `1.5 IS NULL` → `f`  
_`datatype`_ `IS NOT NULL` → `boolean` Test whether value is not null.  `'null' IS NOT NULL` → `t`  
_`datatype`_ `ISNULL` → `boolean` Test whether value is null (nonstandard syntax).   
_`datatype`_ `NOTNULL` → `boolean` Test whether value is not null (nonstandard syntax).   
`boolean` `IS TRUE` → `boolean` Test whether boolean expression yields true.  `true IS TRUE` → `t` `NULL::boolean IS TRUE` → `f` (rather than `NULL`)   
`boolean` `IS NOT TRUE` → `boolean` Test whether boolean expression yields false or unknown.  `true IS NOT TRUE` → `f` `NULL::boolean IS NOT TRUE` → `t` (rather than `NULL`)   
`boolean` `IS FALSE` → `boolean` Test whether boolean expression yields false.  `true IS FALSE` → `f` `NULL::boolean IS FALSE` → `f` (rather than `NULL`)   
`boolean` `IS NOT FALSE` → `boolean` Test whether boolean expression yields true or unknown.  `true IS NOT FALSE` → `t` `NULL::boolean IS NOT FALSE` → `t` (rather than `NULL`)   
`boolean` `IS UNKNOWN` → `boolean` Test whether boolean expression yields unknown.  `true IS UNKNOWN` → `f` `NULL::boolean IS UNKNOWN` → `t` (rather than `NULL`)   
`boolean` `IS NOT UNKNOWN` → `boolean` Test whether boolean expression yields true or false.  `true IS NOT UNKNOWN` → `t` `NULL::boolean IS NOT UNKNOWN` → `f` (rather than `NULL`)   
  
  


The `BETWEEN` predicate simplifies range tests: 
    
    
    _a_ BETWEEN _x_ AND _y_
    

is equivalent to 
    
    
    _a_ >= _x_ AND _a_ <= _y_
    

Notice that `BETWEEN` treats the endpoint values as included in the range. `BETWEEN SYMMETRIC` is like `BETWEEN` except there is no requirement that the argument to the left of `AND` be less than or equal to the argument on the right. If it is not, those two arguments are automatically swapped, so that a nonempty range is always implied. 

The various variants of `BETWEEN` are implemented in terms of the ordinary comparison operators, and therefore will work for any data type(s) that can be compared. 

### Note

The use of `AND` in the `BETWEEN` syntax creates an ambiguity with the use of `AND` as a logical operator. To resolve this, only a limited set of expression types are allowed as the second argument of a `BETWEEN` clause. If you need to write a more complex sub-expression in `BETWEEN`, write parentheses around the sub-expression. 

Ordinary comparison operators yield null (signifying “unknown”), not true or false, when either input is null. For example, `7 = NULL` yields null, as does `7 <> NULL`. When this behavior is not suitable, use the `IS [ NOT ] DISTINCT FROM` predicates: 
    
    
    _a_ IS DISTINCT FROM _b_
    _a_ IS NOT DISTINCT FROM _b_
    

For non-null inputs, `IS DISTINCT FROM` is the same as the `<>` operator. However, if both inputs are null it returns false, and if only one input is null it returns true. Similarly, `IS NOT DISTINCT FROM` is identical to `=` for non-null inputs, but it returns true when both inputs are null, and false when only one input is null. Thus, these predicates effectively act as though null were a normal data value, rather than “unknown”. 

To check whether a value is or is not null, use the predicates: 
    
    
    _expression_ IS NULL
    _expression_ IS NOT NULL
    

or the equivalent, but nonstandard, predicates: 
    
    
    _expression_ ISNULL
    _expression_ NOTNULL
    

Do _not_ write `_`expression`_ = NULL` because `NULL` is not “equal to” `NULL`. (The null value represents an unknown value, and it is not known whether two unknown values are equal.) 

### Tip

Some applications might expect that `_`expression`_ = NULL` returns true if _`expression`_ evaluates to the null value. It is highly recommended that these applications be modified to comply with the SQL standard. However, if that cannot be done the [transform_null_equals](runtime-config-compatible.md#GUC-TRANSFORM-NULL-EQUALS) configuration variable is available. If it is enabled, PostgreSQL will convert `x = NULL` clauses to `x IS NULL`. 

If the _`expression`_ is row-valued, then `IS NULL` is true when the row expression itself is null or when all the row's fields are null, while `IS NOT NULL` is true when the row expression itself is non-null and all the row's fields are non-null. Because of this behavior, `IS NULL` and `IS NOT NULL` do not always return inverse results for row-valued expressions; in particular, a row-valued expression that contains both null and non-null fields will return false for both tests. For example: 
    
    
    SELECT ROW(1,2.5,'this is a test') = ROW(1, 3, 'not the same');
    
    SELECT ROW(table.*) IS NULL FROM table;  -- detect all-null rows
    
    SELECT ROW(table.*) IS NOT NULL FROM table;  -- detect all-non-null rows
    
    SELECT NOT(ROW(table.*) IS NOT NULL) FROM TABLE; -- detect at least one null in rows
    

In some cases, it may be preferable to write _`row`_ `IS DISTINCT FROM NULL` or _`row`_ `IS NOT DISTINCT FROM NULL`, which will simply check whether the overall row value is null without any additional tests on the row fields. 

Boolean values can also be tested using the predicates 
    
    
    _boolean_expression_ IS TRUE
    _boolean_expression_ IS NOT TRUE
    _boolean_expression_ IS FALSE
    _boolean_expression_ IS NOT FALSE
    _boolean_expression_ IS UNKNOWN
    _boolean_expression_ IS NOT UNKNOWN
    

These will always return true or false, never a null value, even when the operand is null. A null input is treated as the logical value “unknown”. Notice that `IS UNKNOWN` and `IS NOT UNKNOWN` are effectively the same as `IS NULL` and `IS NOT NULL`, respectively, except that the input expression must be of Boolean type. 

Some comparison-related functions are also available, as shown in [Table 9.3](functions-comparison.md#FUNCTIONS-COMPARISON-FUNC-TABLE "Table 9.3. Comparison Functions"). 

**Table 9.3. Comparison Functions**

Function  Description  Example(s)   
---  
`num_nonnulls` ( `VARIADIC` `"any"` ) → `integer` Returns the number of non-null arguments.  `num_nonnulls(1, NULL, 2)` → `2`  
`num_nulls` ( `VARIADIC` `"any"` ) → `integer` Returns the number of null arguments.  `num_nulls(1, NULL, 2)` → `1`  
  
  


* * *

[Prev](functions-logical.md "9.1. Logical Operators") | [Up](functions.md "Chapter 9. Functions and Operators")|  [Next](functions-math.md "9.3. Mathematical Functions and Operators")  
---|---|---  
9.1. Logical Operators | [Home](index.md "PostgreSQL 17.5 Documentation")|  9.3. Mathematical Functions and Operators
