30.1. What Is JIT compilation?  
---  
[Prev](jit.md "Chapter 30. Just-in-Time Compilation \(JIT\)") | [Up](jit.md "Chapter 30. Just-in-Time Compilation \(JIT\)")| Chapter 30. Just-in-Time Compilation (JIT)| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](jit-decision.md "30.2. When to JIT?")  
  
* * *

## 30.1. What Is JIT compilation? #

[30.1.1. JIT Accelerated Operations](jit-reason.md#JIT-ACCELERATED-OPERATIONS)
[30.1.2. Inlining](jit-reason.md#JIT-INLINING)
[30.1.3. Optimization](jit-reason.md#JIT-OPTIMIZATION)

Just-in-Time (JIT) compilation is the process of turning some form of interpreted program evaluation into a native program, and doing so at run time. For example, instead of using general-purpose code that can evaluate arbitrary SQL expressions to evaluate a particular SQL predicate like `WHERE a.col = 3`, it is possible to generate a function that is specific to that expression and can be natively executed by the CPU, yielding a speedup. 

PostgreSQL has builtin support to perform JIT compilation using [LLVM](https://llvm.org/) when PostgreSQL is built with [`--with-llvm`](install-make.md#CONFIGURE-WITH-LLVM). 

See `src/backend/jit/README` for further details. 

### 30.1.1. JIT Accelerated Operations #

Currently PostgreSQL's JIT implementation has support for accelerating expression evaluation and tuple deforming. Several other operations could be accelerated in the future. 

Expression evaluation is used to evaluate `WHERE` clauses, target lists, aggregates and projections. It can be accelerated by generating code specific to each case. 

Tuple deforming is the process of transforming an on-disk tuple (see [Section 65.6.1](storage-page-layout.md#STORAGE-TUPLE-LAYOUT "65.6.1. Table Row Layout")) into its in-memory representation. It can be accelerated by creating a function specific to the table layout and the number of columns to be extracted. 

### 30.1.2. Inlining #

PostgreSQL is very extensible and allows new data types, functions, operators and other database objects to be defined; see [Chapter 36](extend.md "Chapter 36. Extending SQL"). In fact the built-in objects are implemented using nearly the same mechanisms. This extensibility implies some overhead, for example due to function calls (see [Section 36.3](xfunc.md "36.3. User-Defined Functions")). To reduce that overhead, JIT compilation can inline the bodies of small functions into the expressions using them. That allows a significant percentage of the overhead to be optimized away. 

### 30.1.3. Optimization #

LLVM has support for optimizing generated code. Some of the optimizations are cheap enough to be performed whenever JIT is used, while others are only beneficial for longer-running queries. See <https://llvm.org/docs/Passes.md#transform-passes> for more details about optimizations. 

* * *

[Prev](jit.md "Chapter 30. Just-in-Time Compilation \(JIT\)") | [Up](jit.md "Chapter 30. Just-in-Time Compilation \(JIT\)")|  [Next](jit-decision.md "30.2. When to JIT?")  
---|---|---  
Chapter 30. Just-in-Time Compilation (JIT) | [Home](index.md "PostgreSQL 17.5 Documentation")|  30.2. When to JIT?
