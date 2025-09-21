34.9. Preprocessor Directives  
---  
[Prev](ecpg-errors.md "34.8. Error Handling") | [Up](ecpg.md "Chapter 34. ECPG — Embedded SQL in C")| Chapter 34. ECPG — Embedded SQL in C| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](ecpg-process.md "34.10. Processing Embedded SQL Programs")  
  
* * *

## 34.9. Preprocessor Directives #

[34.9.1. Including Files](ecpg-preproc.md#ECPG-INCLUDE)
[34.9.2. The define and undef Directives](ecpg-preproc.md#ECPG-DEFINE)
[34.9.3. ifdef, ifndef, elif, else, and endif Directives](ecpg-preproc.md#ECPG-IFDEF)

Several preprocessor directives are available that modify how the `ecpg` preprocessor parses and processes a file. 

### 34.9.1. Including Files #

To include an external file into your embedded SQL program, use: 
    
    
    EXEC SQL INCLUDE _filename_ ;
    EXEC SQL INCLUDE <_filename_ >;
    EXEC SQL INCLUDE "_filename_ ";
    

The embedded SQL preprocessor will look for a file named `_`filename`_.h`, preprocess it, and include it in the resulting C output. Thus, embedded SQL statements in the included file are handled correctly. 

The `ecpg` preprocessor will search a file at several directories in following order: 

  * current directory
  * `/usr/local/include`
  * PostgreSQL include directory, defined at build time (e.g., `/usr/local/pgsql/include`)
  * `/usr/include`



But when `EXEC SQL INCLUDE "_`filename`_ "` is used, only the current directory is searched. 

In each directory, the preprocessor will first look for the file name as given, and if not found will append `.h` to the file name and try again (unless the specified file name already has that suffix). 

Note that `EXEC SQL INCLUDE` is _not_ the same as: 
    
    
    #include <_filename_.h>
    

because this file would not be subject to SQL command preprocessing. Naturally, you can continue to use the C `#include` directive to include other header files. 

### Note

The include file name is case-sensitive, even though the rest of the `EXEC SQL INCLUDE` command follows the normal SQL case-sensitivity rules. 

### 34.9.2. The define and undef Directives #

Similar to the directive `#define` that is known from C, embedded SQL has a similar concept: 
    
    
    EXEC SQL DEFINE _name_ ;
    EXEC SQL DEFINE _name_ _value_ ;
    

So you can define a name: 
    
    
    EXEC SQL DEFINE HAVE_FEATURE;
    

And you can also define constants: 
    
    
    EXEC SQL DEFINE MYNUMBER 12;
    EXEC SQL DEFINE MYSTRING 'abc';
    

Use `undef` to remove a previous definition: 
    
    
    EXEC SQL UNDEF MYNUMBER;
    

Of course you can continue to use the C versions `#define` and `#undef` in your embedded SQL program. The difference is where your defined values get evaluated. If you use `EXEC SQL DEFINE` then the `ecpg` preprocessor evaluates the defines and substitutes the values. For example if you write: 
    
    
    EXEC SQL DEFINE MYNUMBER 12;
    ...
    EXEC SQL UPDATE Tbl SET col = MYNUMBER;
    

then `ecpg` will already do the substitution and your C compiler will never see any name or identifier `MYNUMBER`. Note that you cannot use `#define` for a constant that you are going to use in an embedded SQL query because in this case the embedded SQL precompiler is not able to see this declaration. 

If multiple input files are named on the `ecpg` preprocessor's command line, the effects of `EXEC SQL DEFINE` and `EXEC SQL UNDEF` do not carry across files: each file starts with only the symbols defined by `-D` switches on the command line. 

### 34.9.3. ifdef, ifndef, elif, else, and endif Directives #

You can use the following directives to compile code sections conditionally: 

`EXEC SQL ifdef _`name`_ ;` #
    

Checks a _`name`_ and processes subsequent lines if _`name`_ has been defined via `EXEC SQL define _`name`_`. 

`EXEC SQL ifndef _`name`_ ;` #
    

Checks a _`name`_ and processes subsequent lines if _`name`_ has _not_ been defined via `EXEC SQL define _`name`_`. 

`EXEC SQL elif _`name`_ ;` #
    

Begins an optional alternative section after an `EXEC SQL ifdef _`name`_` or `EXEC SQL ifndef _`name`_` directive. Any number of `elif` sections can appear. Lines following an `elif` will be processed if _`name`_ has been defined _and_ no previous section of the same `ifdef`/`ifndef`...`endif` construct has been processed. 

`EXEC SQL else;` #
    

Begins an optional, final alternative section after an `EXEC SQL ifdef _`name`_` or `EXEC SQL ifndef _`name`_` directive. Subsequent lines will be processed if no previous section of the same `ifdef`/`ifndef`...`endif` construct has been processed. 

`EXEC SQL endif;` #
    

Ends an `ifdef`/`ifndef`...`endif` construct. Subsequent lines are processed normally. 

`ifdef`/`ifndef`...`endif` constructs can be nested, up to 127 levels deep. 

This example will compile exactly one of the three `SET TIMEZONE` commands: 
    
    
    EXEC SQL ifdef TZVAR;
    EXEC SQL SET TIMEZONE TO TZVAR;
    EXEC SQL elif TZNAME;
    EXEC SQL SET TIMEZONE TO TZNAME;
    EXEC SQL else;
    EXEC SQL SET TIMEZONE TO 'GMT';
    EXEC SQL endif;
    

* * *

[Prev](ecpg-errors.md "34.8. Error Handling") | [Up](ecpg.md "Chapter 34. ECPG — Embedded SQL in C")|  [Next](ecpg-process.md "34.10. Processing Embedded SQL Programs")  
---|---|---  
34.8. Error Handling | [Home](index.md "PostgreSQL 17.5 Documentation")|  34.10. Processing Embedded SQL Programs
