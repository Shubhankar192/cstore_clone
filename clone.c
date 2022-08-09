#include "postgres.h"
#include "executor/spi.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;



PG_FUNCTION_INFO_V1(clone);



//My Main Function, Responsible for clonning.

Datum clone(PG_FUNCTION_ARGS) {

    char *table_name  = text_to_cstring(PG_GETARG_TEXT_PP(0));
    uint64 proc;

    char *command ;

// Constructing create foreign table command

    command = (char *)palloc(2*strlen(table_name) + 65);
    snprintf(command , 2*strlen(table_name) + 65 , "create foreign table clone_%s() inherits(%s) server cstore_server", table_name , table_name);

 

//SPI connertection started
    SPI_connect();      
    proc = SPI_processed;
    int ret = SPI_exec(command,1);

//ALTERING TABLE

    char *alterTable;
    alterTable = (char *)palloc(2*strlen(table_name) + 36);
    snprintf(alterTable ,2*strlen(table_name) + 36 , "ALTER TABLE clone_%s  NO INHERIT %s" , table_name , table_name);

    int data2 = SPI_exec(alterTable , 1);
    
//For Copying the schema of table and extracting the columns names of the given table.

    char *print_row = (char *)palloc(strlen(table_name) + 26);
    snprintf(print_row ,strlen(table_name) + 26 , "select * from %s limit 0", table_name);

    char buf[8192] ;
    int dat = SPI_exec(print_row,1);
    if(dat !=0 && SPI_tuptable != NULL) {

        SPITupleTable *tuptable = SPI_tuptable;
        TupleDesc tupdesc = tuptable->tupdesc;

        buf[0] = 0;
        for(int i = 1 ; i <= tupdesc->natts ; i++) {
            char *s = SPI_fname(tupdesc,i);
            snprintf(buf + strlen(buf) , sizeof(buf)-strlen(buf) , "'''||NEW.%s||'''%s",s ,(i == tupdesc->natts) ? "":",");
        }
        //elog(INFO, "%s",buf);
        elog(INFO , "Clonning Table");

    }

// Triggger Function : Function which will get called, whenever a trigger is fired.

    // char *function = (char*)palloc(4*strlen(table_name)+227+strlen(buf));
    // snprintf(function  
    // ,4*strlen(table_name)+227+strlen(buf),
    // "CREATE or REPLACE FUNCTION clone_data_%s() RETURNS TRIGGER AS $$ BEGIN EXECUTE 'COPY (select %s) to ''/home/data/%s.csv'' with CSV;' USING NEW; COPY clone_%s from '/home/data/%s.csv' delimiter ','; RETURN NEW; END; $$ language 'plpgsql';" 
    // ,table_name
    // ,buf               
    // ,table_name
    // ,table_name
    // ,table_name);


// Executing Trigger  Function

    // int ret1 = SPI_exec(function , 1);

    // if(ret1 != 0 )
    //     elog(INFO ,"Creating Functions and triggers");


// Inserting Trigger to our Source  Table

    // For each table a specific trigger will get created.
    // which will get fired when we insert a new row into our parent table.
    // so for n tables , n triggers will also  get genereated. Once a trigger gets fired, 
    // it will execute a common function which will copy the data from source to destination.
    
    char * trigger = (char*)palloc(strlen(table_name) + 88);

    snprintf(
    trigger ,
    strlen(table_name) + 88, 
    "create trigger clone_row after insert on %s for each row execute procedure trig_test();" 
    ,table_name 
    ,table_name
    ,table_name );
                                                                                   

    int ret3 = SPI_exec(trigger , 1);
    if(ret3 != 0 )
        elog(INFO , "Trigger Generated");

    elog(INFO , "Clone Created Successfully !");
                                  
 
    SPI_finish();  //Connection Closed and free the memory!
    pfree(command);
    pfree(trigger);
   // pfree(function);
    pfree(print_row);
   
    // pfree(copy_to);
    // pfree(copy_from);

    PG_RETURN_INT64(proc);
}





















//Constructing Copy_to Command
//     char *copy_to = (char *)palloc(2*strlen(table_name) + 37); 
//     snprintf(copy_to , 2*strlen(table_name) + 37 , "COPY %s to '/home/data/%s.csv' with CSV", table_name , table_name);



// //Constructing Copy_from  Command
//     char *copy_from = (char *)palloc(2*strlen(table_name) + 49);
//     snprintf(copy_from , 2*strlen(table_name) + 49, "COPY clone_%s from '/home/data/%s.csv' delimiter ','", table_name , table_name);

 

//     int ret1 = SPI_exec(copy_to , 1);
//     if(ret1 != 0 )
//         elog(INFO , "Copying  Data to csv!", ret1);
//     int ret2 = SPI_exec(copy_from ,1);
//     if(ret2 != 0 )
//         elog(INFO , "Copying the data from csv to clone" ,ret2);



    // strcat(command ,"create foreign table clone_");
    // strcat(command ,table_name );
    // strcat(command , "() inherits(");
    // strcat(command ,table_name);
    // strcat(command , ") server cstore_server"); 


    // strcat(copy_to , "COPY ");
    // strcat(copy_to , table_name); 
    // strcat(copy_to , " to '/home/data/");
    // strcat(copy_to , table_name);
    // strcat(copy_to ,".csv' with CSV");


    // strcat(copy_from , "COPY clone_");
    // strcat(copy_from , table_name);  
    // strcat(copy_from , " from '/home/data/");
    // strcat(copy_from , table_name);
    // strcat(copy_from , ".csv' delimiter ',' ");