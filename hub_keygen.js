// Snowflake version of Teradata's hub_keygen - this one allows update on source or write to target
//
// TODO: add more info to this header
// sample call: call hub_keygen('sw_test', 'sf_test', 'KEY:key1,key2; SYS:number1', 'b_test');
//
// v1.0  PG     Apr 21  Initial version
//  "    PG  23 Apr 21  Allow PRIMARY KEY columns to be NULL (for Tony)

create or replace table s_test (
   --primary key
   key1                         varchar not null,
   key2                         varchar not null,
   number1                      number,
   icw_eff_ts                   timestamp,
   icw_exp_ts                   timestamp
)
;

create or replace procedure hub_keygen( SRC     string,
                                        TGT     string,
                                        ROLES   string,
                                        DRIVER  string )
    returns string
    language javascript
    comment = 'Snowflake version of sp_hub_keygen'
    as
$$
'use strict';

const PROC_NM = 'hub_keygen v1.0';

// later retrieved from current_warehouse(), etc
let WAREHOUSE = '', DATABASE = '', SCHEMA = '', USER = '', ROLE = '';

// this makes me a bit queasy, but simplest way to cache last log message as return message from proc
let LAST_LOG_MESSAGE = '';


// log message to table - details might be the SQL about to be run or a stack trace
function log(msg, details=null) {
    LAST_LOG_MESSAGE = msg;
    snowflake.execute({
        sqlText: 'insert into audit_log(src_nm, user_nm, role_nm, wh_nm, msg_txt, extra_txt) values(:1, :2, :3, :4, :5, :6)',
        binds: [PROC_NM, USER, ROLE, WAREHOUSE, msg, details]
    });
}

// parse table name into [database.][schema.]table
function parse_name(label, nm, def_db, def_schema) {
    let database = def_db, schema = def_schema, table = nm;
    const parts = nm.toUpperCase().split('.');

    if (parts.length === 3)
        [database, schema, table] = parts;
    else if (parts.length === 2)
        [schema, table] = parts;
    else if (parts.length === 1)
        table = parts[0];
    else if (nm.trim() === '')
        throw new SyntaxError(`${label} table name not provided`);
    else
        throw new SyntaxError(`${label} table name invalid (${nm})`);

    return [database, schema, table];
}

// helper to add a prefix to all entries in a list and return them as a comma separated string
function prefixAndIndent(list, prefix, indent=0) {

    return list.map(e => prefix + e).join(',\n'+ ' '.repeat(indent) )
}

// get current warehouse, database, schema, user and role
function getEnv() {
    let sql = 'SELECT current_warehouse(), current_database(), current_schema(), current_user(), current_role()';
    let stmt = snowflake.createStatement( {sqlText: sql} )
    let r = stmt.execute();
    if (r.next())
        return [r.getColumnValue(1), r.getColumnValue(2), r.getColumnValue(3), r.getColumnValue(4), r.getColumnValue(5)]
}


// the main show ...
//    'improved' from Teradata version - can have a target table (Teradata merges into source) and takes parameters more akin to merge()
//     ie key_gen(source, target, 'key:k1,k2,k3; sys: my_surrogate_id', 'b_base')       -- TODO: maybe use mnemonic other than 'sys'
//     update is expensive, prefer to have a new target rather than update generated keys into source
try {

    // what's our environment and parameters
    [WAREHOUSE, DATABASE, SCHEMA, USER, ROLE] = getEnv()
    log('Environment:', `Warehouse: ${WAREHOUSE}, Database ${DATABASE}, Schema: ${SCHEMA}, User: ${USER}, Role: ${ROLE}`);

    const [src_db_nm, src_schema, src_tbl_nm] = parse_name('Source', SRC,              DATABASE, SCHEMA);
    const [tgt_db_nm, tgt_schema, tgt_tbl_nm] = parse_name('Target', !TGT ? SRC : TGT, DATABASE, SCHEMA);
    const [dvr_db_nm, dvr_schema, dvr_tbl_nm] = parse_name('Store',  DRIVER,           DATABASE, SCHEMA);
    log('Parameters:', `Source: ${src_db_nm}.${src_schema}.${src_tbl_nm}, Target: ${tgt_db_nm}.${tgt_schema}.${tgt_tbl_nm}`);
    log(`Keys will be added to the ${(!TGT) ? 'source' : 'target'} table`);


    // dictionary query - constructed from a Javascript template string (like a Python f-string)
    let sql = `
        SELECT coalesce(src.column_name, '')     as src_col_nm,
               tgt.column_name                   as tgt_col_nm,
               rol.role_name                     as column_role

          FROM ( -- source table)
                  SELECT table_name,
                         ordinal_position,
                         column_name,
                         data_type
                    FROM ${src_db_nm}.information_schema.columns s
                   WHERE table_catalog = '${src_db_nm}'
                     AND table_schema  = '${src_schema}'
                     AND table_name    = '${src_tbl_nm}'
                ) src

      FULL JOIN ( -- target table
                  SELECT table_name,
                         ordinal_position,
                         column_name,
                         data_type
                    FROM ${tgt_db_nm}.information_schema.columns s
                   WHERE table_catalog = '${tgt_db_nm}'
                     AND table_schema  = '${tgt_schema}'
                     AND table_name    = '${tgt_tbl_nm}'
                ) tgt

             ON src.column_name = tgt.column_name

      LEFT JOIN ( -- Roles are given in the form:  label1: value,value... [; label:value, value...]
                  --   eg KEY:col1,col2; SYS:my_new_id
                  --   Teradata version uses sp_parse_framework_params() and gt_framework_parameters
                  SELECT trim(l.value) as column_name,
                         trim(k.key)   as role_name

                    FROM ( -- firstly split roles on semicolon delimiter
                            SELECT index,
                                   position(':' in value)       as p,
                                   substring(value, 1, p-1)     as key,
                                   substring(value, p+1)        as value
                              FROM table (
                                  strtok_split_to_table( upper('${ROLES}'), ';')
                              )
                         ) k,
                         -- then on various allowable delimiters
                         lateral strtok_split_to_table(k.value, ', []')  l
                 ) rol
             ON rol.column_name = coalesce(tgt.column_name, src.column_name)

       ORDER BY coalesce(src.ordinal_position, tgt.ordinal_position)
    `;
    log('Dictionary query:', sql);

    // snowflake stuff to prepare and then run a query
    let stmt = snowflake.createStatement( {sqlText: sql} )
    let results = stmt.execute();

    // sanity check results
    if (stmt.getColumnCount() === 0)
        throw new ReferenceError(`Source and/or Target tables not found`);   // TODO: check driver also exists

    const key_list = [];
    const sys_list = [];
    const oth_list = [];

    // cycle thru each column's details
    while (results.next()) {

        let src_col_nm     = results.getColumnValue('SRC_COL_NM');
        let tgt_col_nm     = results.getColumnValue('TGT_COL_NM');
        let column_role    = results.getColumnValue('COLUMN_ROLE');

        if (src_col_nm === null)
            log(`WARNING: Target column ${tgt_col_nm} not found on source table`);

        if (tgt_col_nm === null)
            throw new ReferenceError(`Required column ${src_col_nm} not found on target table`);

        if (column_role === 'KEY')
            key_list.push(tgt_col_nm)

        else if (column_role === 'SYS')
            sys_list.push(tgt_col_nm)

        else
            oth_list.push(tgt_col_nm);
    }

    // Sanity checks
    if (key_list.length === 0) throw new ReferenceError(`Required KEY field(s) not found in target`);
    if (sys_list.length !== 1) throw new ReferenceError(`One and only one SYS field permitted`);


    // 1 of 5: Cache existing/known keys
    let existing_sql = `
        CREATE OR REPLACE TEMPORARY TABLE key_cache AS (

        SELECT ${prefixAndIndent(key_list, 's.',  15)},
               d.${sys_list[0]}

          FROM ( -- all the keys we need to find
                 SELECT DISTINCT ${prefixAndIndent(key_list, '',  33)}
                   FROM ${src_db_nm}.${src_schema}.${src_tbl_nm} ) s

     LEFT JOIN ${dvr_db_nm}.${dvr_schema}.${dvr_tbl_nm} d
            ON ${key_list.map(e => 'd.' + e + ' = s.' + e).join('\n' + ' '.repeat(11) + 'AND ')}

           AND d.icw_exp_ts > CURRENT_TIMESTAMP     -- current target records only

        )`;


    // 2 of 5: Add in new keys - beware of Snowflake vs. Teradata UPDATE syntax
    let new_sql = `
        UPDATE key_cache c

          FROM ( -- generate new keys where needed
                  SELECT ${prefixAndIndent(key_list, 'k.',  25)},
                         m.max_key + row_number() over ( order by null ) as key_no
                    FROM key_cache k

          CROSS JOIN ( SELECT zeroifnull( MAX(${sys_list[0]}) ) as max_key
                         FROM ${dvr_db_nm}.${dvr_schema}.${dvr_tbl_nm} ) m

               WHERE k.${sys_list[0]} is null

                ) u

           SET ${sys_list[0]} = u.key_no

         WHERE ${key_list.map(e => 'u.' + e + ' = c.' + e).join('\n' + ' '.repeat(11) + 'AND ')}

        `;


    // Now we have two possibilities - (a) Follow Teradata version and update the keys back onto the source table
    //                              or (b) Join source to the cached keys and write to target table

    let final_sql1 = `
        UPDATE ${src_db_nm}.${src_schema}.${src_tbl_nm} s

          FROM key_cache c

           SET ${sys_list[0]} = c.${sys_list[0]}

         WHERE ${key_list.map(e => 's.' + e + ' = c.' + e).join('\n' + ' '.repeat(11) + 'AND ')}
        `;

    let final_sql2 = `
        INSERT INTO ${tgt_db_nm}.${tgt_schema}.${tgt_tbl_nm} (
               ${prefixAndIndent(key_list, '',  15)},
               ${prefixAndIndent(oth_list, '',  15)},
               ${sys_list[0]}
        )

        SELECT ${prefixAndIndent(key_list, 's.',  15)},
               ${prefixAndIndent(oth_list, 's.',  15)},
               c.${sys_list[0]}

          FROM ${src_db_nm}.${src_schema}.${src_tbl_nm} s

          JOIN key_cache c
            ON ${key_list.map(e => 'c.' + e + ' = s.' + e).join('\n' + ' '.repeat(11) + 'AND ')}
        `;


    // we now have our 3 sql statements, let's run 'em - via a cover function that'll also log some details
    function logAndRun(msg, sql, msg2=undefined, cmd=undefined, table=undefined) {
        log(msg, sql);
        let s = snowflake.createStatement( {sqlText: sql}  )
        let r = s.execute();

        // do extra stuff depending on cmd
        if (cmd === 'get_count')
            r = snowflake.execute( {sqlText: `SELECT count(*) FROM ${table}`} );

        if (cmd === 'get_count' || cmd === 'row_count')
            while (r.next())
                log(`${msg2} ${r.getColumnValue(1)}`);
    }


    logAndRun('Existing keys sql:',  existing_sql, 'Keys needed:',        'get_count',  'key_cache');
    logAndRun('New keys sql:',       new_sql,      'New keys generated:', 'row_count');

    if (TGT) {
        snowflake.execute( {sqlText: `DELETE FROM ${TGT}`} );
        logAndRun('Final sql:', final_sql2, `Records inserted to ${tgt_tbl_nm}:`, 'row_count');
    } else
        logAndRun('Final sql:', final_sql1, `Records updated on ${src_tbl_nm}:`,  'row_count');

    return 'OK';
}

catch (e) {
    log(`ERROR: (${e.name}) ${e.message}`, e.stack);
    return `${e.message}`;
}
$$
;