// Snowflake version of Jon's normal_merge crossed with Paul Shade's temporal_merge
//
// The Teradata version evolved separately to temporal_merge - this one is based on temporal_merge
// It had two modes: DELTA and ALL. This one switches to ALL if a WINDOWing expression is given
// TODO: add more info to this header
// sample call: call standard_merge('t_test', 'b_test', 'KEY:key1,key2,key3',  'acg_year_no=2021');
//
// v1.0  PG     Apr 21  Initial version
//  "    PG  23 Apr 21  Allow PRIMARY KEY columns to be NULL (for Tony)

create or replace procedure standard_merge( SRC     string,     // source [db.][schema.]tablename
                                            TGT     string,     // target [db.][schema.]tablename
                                            ROLES   string,     // column roles (KEY, BUS, SYS, OPR)
                                            WINDOW  string )    // optional window/where expression (forces ALL)
    returns string
    language javascript
    comment = 'Snowflake version of Jon''s normal_merge cross-bred with Paul Shade''s temporal_merge'
    as
$$
'use strict';

const PROC_NM = 'standard_merge v1.0';

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

// get current warehouse, database and schema
function getEnv() {
    let sql = 'SELECT current_warehouse(), current_database(), current_schema(), current_user(), current_role()';
    let stmt = snowflake.createStatement( {sqlText: sql} )
    let r = stmt.execute();
    if (r.next())
        return [r.getColumnValue(1), r.getColumnValue(2), r.getColumnValue(3), r.getColumnValue(4), r.getColumnValue(5)]
}


// the main show ...
try {

    // what's our environment and parameters
    [WAREHOUSE, DATABASE, SCHEMA, USER, ROLE] = getEnv()
    log('Environment:', `Warehouse: ${WAREHOUSE}, Database ${DATABASE}, Schema: ${SCHEMA}, User: ${USER}, Role: ${ROLE}`);

    const [src_db_nm, src_schema, src_tbl_nm] = parse_name('Source', SRC, DATABASE, SCHEMA);
    const [tgt_db_nm, tgt_schema, tgt_tbl_nm] = parse_name('Target', TGT, DATABASE, SCHEMA);
    log('Parameters:', `Source: ${src_db_nm}.${src_schema}.${src_tbl_nm}, Target: ${tgt_db_nm}.${tgt_schema}.${tgt_tbl_nm}`);
    (!WINDOW) ? log('No windowing expression - DELTA merge enabled') : ('Windowing expression:', `${WINDOW}`);


    // dictionary query - constructed from a Javascript template string (like a Python f-string)
    const sql = `
        SELECT coalesce(src.column_name, '')                                                  as src_col_nm,
               tgt.column_name                                                                as tgt_col_nm,
               CASE when rol.role_name is not null                       THEN rol.role_name
                    when tgt.column_name in ('ICW_EFF_TS', 'ICW_EXP_TS') THEN 'SYS'
                                                                         ELSE 'HIS'
               END                                                                            as column_role,
               columntype_default(tgt.data_type)                                              as column_default

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

      LEFT JOIN ( -- Roles are given in the form:  label1: value,value... [; label2:value, value...]
                  --   eg KEY:col1,col2; SYS:icw_eff_ts, icw_exp_ts
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
    if (stmt.getRowCount() === 0)
        throw new ReferenceError(`Source and/or Target tables not found`);

    const key_list = [];
    const his_list = [];
    const opr_list = [];
    const sys_list = [];
    const oth_list = [];
    const key_defl = [];   // defaults for coalesce of KEY columns
    const his_defl = [];   //     "     "      "    "  HIS   "

    // cycle thru each column's details
    while (results.next()) {

        let src_col_nm     = results.getColumnValue('SRC_COL_NM');
        let tgt_col_nm     = results.getColumnValue('TGT_COL_NM');
        let column_role    = results.getColumnValue('COLUMN_ROLE');
        let column_default = results.getColumnValue('COLUMN_DEFAULT');

        if (tgt_col_nm === null && column_role !== 'OPR' )
            log(`WARNING: Source column ${src_col_nm} not found on target table`);

        if (src_col_nm === null )
            throw new ReferenceError(`Required column ${tgt_col_nm} not found on source table`);

        if (column_role === 'KEY') {
            key_list.push(tgt_col_nm);
            key_defl.push(column_default);
        }
        else if (column_role === 'HIS') {
            his_list.push(tgt_col_nm);
            his_defl.push(column_default)
        }
        else if (column_role === 'OPR')
            opr_list.push(src_col_nm);

        else if (column_role === 'UPD')
            throw new ReferenceError(`The UPD role is no longer supported`);

        else if (column_role === 'BUS')
            throw new ReferenceError(`The BUS role is not supported by this function - please use temporal_merge()`);

        else if (column_role === 'SYS')
            sys_list.push(tgt_col_nm)

        else
            oth_list.push(column_role);

    }

    // Sanity checks
    if (oth_list.length   > 0) throw new ReferenceError(`Unknown role(s) detected - ${oth_list.filter((v, i, s) => s.indexOf(v) === i)}`);  // unique   TODO: use a Set
    if (key_list.length === 0) throw new ReferenceError(`Required KEY field(s) not found in target`);
    if (sys_list.length !== 2) throw new ReferenceError(`Required SYS field(s) not found in target`);
    if (opr_list.length   > 1) throw new ReferenceError(`Only one Logical-delete (OPR) fields allowed`);



    // formatting helper: add a prefix to all entries in a list and return them as a comma separated indented string
    function prefixAndIndent(list, prefix, indent=0) {
        return list.map(e => prefix + e).join(',\n'.padEnd(indent) );
    }

    // formatting helper: create strings like; COALESCE(p1.col,'') = COALESCE(p2.col,'') AND ...
    function compareEach(list, defl, p1, p2, indent=0) {
        return list.map( (e,i) => `COALESCE(${p1}${e}, ${defl[i]})`.padEnd(50) + `= COALESCE(${p2}${e}, ${defl[i]})`)
                                . join('\n' + ' '.repeat(indent) + 'AND ');
    }



    // This is based on a cut down version of temporal merge - with business period/ranges removed
    // but has grafted-on fuctionality of the Teradata version (delta vs. full merge within optional window)

    // 1 of 5: input_set sql  - unlike temporal_merge we don't need a subquery
    let input_sql = `
        CREATE OR REPLACE TEMPORARY TABLE input_set AS (

         -- incoming source records
        SELECT -- key columns
               ${prefixAndIndent(key_list, '',  17)},
               -- history columns
               ${prefixAndIndent(his_list, '',  17)},

               0                                         as prog_flag

          FROM ${src_db_nm}.${src_schema}.${src_tbl_nm} inp
    `;

    // possibly deal with logically deleted records (role OPR)
    if (opr_list.length !== 0) {

        input_sql += `
          WHERE coalesce(${opr_list[0]}, '') <> 'E'

        UNION

        -- target records to expire
        SELECT  -- key columns
               ${prefixAndIndent(key_list, 'tgt.',  17)},
               -- history columns
               ${prefixAndIndent(his_list, 'tgt.',  17)},

               1                                         as prog_flag

          FROM ${src_db_nm}.${src_schema}.${src_tbl_nm} inp

          -- join source and target on key columns
          JOIN ${tgt_db_nm}.${tgt_schema}.${tgt_tbl_nm} tgt
            ON ${compareEach(key_list, key_defl, 'inp.', 'tgt.', 21)}

         WHERE tgt.${sys_list[1]} > CURRENT_TIMESTAMP       -- current target records only
           AND src.${opr_list[0]} = 'E'                     -- where source OPR column asks for expiry
        `;
    }

    // close off the statement
    input_sql += `
        )`;



    // 2 of 5: change_set sql
    let change_sql = `
        CREATE OR REPLACE TEMPORARY TABLE change_set AS (

        -- 1. all records from input_set
        SELECT -- key columns
               ${prefixAndIndent(key_list, '',  17)},
               -- history columns
               ${prefixAndIndent(his_list, '',  17)},

               prog_flag,
               1                                                  as statement_no

          FROM input_set
        `;

    // unless we're doing a delta merge, include all target records within window
    if (WINDOW) {

        change_sql += `
         UNION

        -- 2. records from target that are inside the window
        SELECT -- key columns
               ${prefixAndIndent(key_list, 'tgt.',  17)},
               -- history columns
               ${prefixAndIndent(his_list, 'tgt.',  17)},

               1                                                  as prog_flag,
               2                                                  as statement_no

          FROM ${tgt_db_nm}.${tgt_schema}.${tgt_tbl_nm} tgt

     LEFT JOIN input_set inp
            ON ${compareEach(key_list, key_defl, 'inp.', 'tgt.', 11)}

         WHERE tgt.${sys_list[1]} > CURRENT_TIMESTAMP       -- current target records only

           AND inp.${key_list[0]} IS NULL                   -- not in source

           -- operate within this window definition
           AND ${WINDOW}
        `;
    }

    // close off the statement
    change_sql += `
        )`;


    // 3 of 5: normal_set sql  - not applicable

    // 4 of 5: merge_set - final data to be used for merging into target - split the SQL to deal with WINDOW
    let merge_sql = `
        CREATE OR REPLACE TEMPORARY TABLE merge_set AS (

        -- changed records from input_set (with prog_flag=0) and system generated effectivity
        SELECT -- key columns
               ${prefixAndIndent(key_list, 'inp.',  17)},
               -- history columns
               ${prefixAndIndent(his_list, 'inp.',  17)},
               -- system generated effectivity
               CURRENT_TIMESTAMP::timestamp_ntz             as ${sys_list[0]},
               '9999-12-31 23:59:59.999'::timestamp_ntz     as ${sys_list[1]}

          FROM change_set inp

     LEFT JOIN ${tgt_db_nm}.${tgt_schema}.${tgt_tbl_nm} tgt
            ON ${compareEach(key_list, key_defl, 'tgt.', 'inp.', 11)}

           -- history columns - ony include if any columns differ - this bit is not in temporal_merge
           AND NOT (
                   ${compareEach(his_list, his_defl, 'inp.', 'tgt.', 15)}
                   )
         WHERE tgt.${sys_list[1]} > CURRENT_TIMESTAMP       -- current target records only
           AND inp.prog_flag = 0                            -- directly from source

         UNION

        -- target records that now have to expire
        SELECT -- key columns
               ${prefixAndIndent(key_list, 'tgt.',  15)},
               -- history columns
               ${prefixAndIndent(his_list, 'tgt.',  15)},
               -- existing effective, system generated expiry
               tgt.${sys_list[0]}::timestamp_ntz,
               CURRENT_TIMESTAMP::timestamp_ntz             as ${sys_list[1]}

          FROM change_set inp

     LEFT JOIN ${tgt_db_nm}.${tgt_schema}.${tgt_tbl_nm} tgt
            ON ${compareEach(key_list, key_defl, 'tgt.', 'inp.', 11)}

           -- history columns - ony include if any columns differ - this bit is not in temporal_merge
           AND NOT (
                   ${compareEach(his_list, his_defl, 'inp.', 'tgt.', 15)}
                   )
         WHERE tgt.${sys_list[1]} > CURRENT_TIMESTAMP       -- current target records only
           AND inp.prog_flag = 0                            -- directly from source
        `;

    if (WINDOW) {
        merge_sql += `
         UNION

        -- round up and expire all our orphan records in window
        SELECT -- key columns
               ${prefixAndIndent(key_list, 'tgt.',  17)},
               -- history columns
               ${prefixAndIndent(his_list, 'tgt.',  17)},
               -- existing effective, system generated expiry
               tgt.${sys_list[0]}::timestamp_ntz,
               CURRENT_TIMESTAMP::timestamp_ntz             as ${sys_list[1]}

          FROM ${tgt_db_nm}.${tgt_schema}.${tgt_tbl_nm} tgt

          JOIN change_set inp
            ON ${compareEach(key_list, key_defl, 'inp.', 'tgt.', 11)}

         WHERE tgt.${sys_list[1]} > CURRENT_TIMESTAMP       -- current target records only
           AND inp.prog_flag = 1                            -- not directly from source
        `;
    }

    // close off the statement
    merge_sql += `
        )`;


    // 5 of 5: final merge  TOTO: check if can change temporal merge to join on EFF_TS rather than all HIS columns
    let final_sql = `
        MERGE INTO ${tgt_db_nm}.${tgt_schema}.${tgt_tbl_nm} tgt

             USING merge_set mer
                -- key columns
                ON ${compareEach(key_list, key_defl, 'tgt.', 'mer.', 15)}

               -- include start/effective time to avoid checking all HIS columns again (cf. temporal_merge)
               AND tgt.${sys_list[0]} = mer.${sys_list[0]}

               AND tgt.${sys_list[1]} > CURRENT_TIMESTAMP       -- current target records only


        WHEN MATCHED AND tgt.${sys_list[1]} <> mer.${sys_list[1]} THEN UPDATE
               SET ${sys_list[1]} = mer.${sys_list[1]}


        WHEN NOT MATCHED THEN INSERT (
            -- key columns
            ${prefixAndIndent(key_list, '',  14)},
            -- history columns
            ${prefixAndIndent(his_list, '',  14)},
            -- effectivity columns
            ${prefixAndIndent(sys_list, '',  14)}
        )
        VALUES (
            -- key columns
            ${prefixAndIndent(key_list, 'mer.',  14)},
            -- history columns
            ${prefixAndIndent(his_list, 'mer.',  14)},
            -- effectivity columns
            ${prefixAndIndent(sys_list, 'mer.',  14)}
        )
    `;


    // we now have our 5 sql statements, let's run 'em - via a cover function that'll also log some details
    function logAndRun(msg, sql, hasResults=false) {
        log(msg, sql);
        let s = snowflake.createStatement( {sqlText: sql}  )
        let r = s.execute();
        // the Snowflake merge statement returns an insert and updated count
        if (hasResults)
            while (r.next())
                log(`Merge - rows inserted: ${r.getColumnValue(1)}, rows updated: ${r.getColumnValue(2)}`);
    }

    logAndRun('Generated input_set sql',  input_sql);
    logAndRun('Generated change_set sql', change_sql);
    logAndRun('Generated merge_set sql',  merge_sql);
    logAndRun('Generated final sql',      final_sql,  true);

    return LAST_LOG_MESSAGE;
}

catch (e) {
    log(`ERROR: (${e.name}) ${e.message}`, e.stack);
    return `${e.message}`;
}
$$
;