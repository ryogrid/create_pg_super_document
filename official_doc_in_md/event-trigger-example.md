38.4. A Complete Event Trigger Example  
---  
[Prev](event-trigger-interface.md "38.3. Writing Event Trigger Functions in C") | [Up](event-triggers.md "Chapter 38. Event Triggers")| Chapter 38. Event Triggers| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](event-trigger-table-rewrite-example.md "38.5. A Table Rewrite Event Trigger Example")  
  
* * *

## 38.4. A Complete Event Trigger Example #

Here is a very simple example of an event trigger function written in C. (Examples of triggers written in procedural languages can be found in the documentation of the procedural languages.) 

The function `noddl` raises an exception each time it is called. The event trigger definition associated the function with the `ddl_command_start` event. The effect is that all DDL commands (with the exceptions mentioned in [Section 38.1](event-trigger-definition.md "38.1. Overview of Event Trigger Behavior")) are prevented from running. 

This is the source code of the trigger function: 
    
    
    #include "postgres.h"
    
    #include "commands/event_trigger.h"
    #include "fmgr.h"
    
    PG_MODULE_MAGIC;
    
    PG_FUNCTION_INFO_V1(noddl);
    
    Datum
    noddl(PG_FUNCTION_ARGS)
    {
        EventTriggerData *trigdata;
    
        if (!CALLED_AS_EVENT_TRIGGER(fcinfo))  /* internal error */
            elog(ERROR, "not fired by event trigger manager");
    
        trigdata = (EventTriggerData *) fcinfo->context;
    
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("command \"%s\" denied",
                        GetCommandTagName(trigdata->tag))));
    
        PG_RETURN_NULL();
    }
    

After you have compiled the source code (see [Section 36.10.5](xfunc-c.md#DFUNC "36.10.5. Compiling and Linking Dynamically-Loaded Functions")), declare the function and the triggers: 
    
    
    CREATE FUNCTION noddl() RETURNS event_trigger
        AS 'noddl' LANGUAGE C;
    
    CREATE EVENT TRIGGER noddl ON ddl_command_start
        EXECUTE FUNCTION noddl();
    

Now you can test the operation of the trigger: 
    
    
    =# \dy
                         List of event triggers
     Name  |       Event       | Owner | Enabled | Function | Tags
    -------+-------------------+-------+---------+----------+------
     noddl | ddl_command_start | dim   | enabled | noddl    |
    (1 row)
    
    =# CREATE TABLE foo(id serial);
    ERROR:  command "CREATE TABLE" denied
    

In this situation, in order to be able to run some DDL commands when you need to do so, you have to either drop the event trigger or disable it. It can be convenient to disable the trigger for only the duration of a transaction: 
    
    
    BEGIN;
    ALTER EVENT TRIGGER noddl DISABLE;
    CREATE TABLE foo (id serial);
    ALTER EVENT TRIGGER noddl ENABLE;
    COMMIT;
    

(Recall that DDL commands on event triggers themselves are not affected by event triggers.) 

* * *

[Prev](event-trigger-interface.md "38.3. Writing Event Trigger Functions in C") | [Up](event-triggers.md "Chapter 38. Event Triggers")|  [Next](event-trigger-table-rewrite-example.md "38.5. A Table Rewrite Event Trigger Example")  
---|---|---  
38.3. Writing Event Trigger Functions in C | [Home](index.md "PostgreSQL 17.5 Documentation")|  38.5. A Table Rewrite Event Trigger Example
