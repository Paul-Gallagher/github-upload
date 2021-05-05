// Snowflake version of lander - create a base table and pipe to load it - can't create a view on top till we have some data
//
// TODO: add more info to this header
// sample call: land_file('Skyscanner/search', 'sky_searches', '{ "format":"parquet", "stage":"jk_db.landing.ba_sandbox_aligned_stage/ICW" }');
//
// v1.0  PG     Apr 21  Initial version

create or replace procedure land_file ( SRC     string,     // source pathname - appended to the STAGE address
                                        TGT     string,     // target [db.][schema.]tablename  -> LF_target_table
                                        PARAMS  string )    // params as JSON string (needs "s)
    returns string
    language javascript
    comment = 'Snowflake version of lander'
    as
$$
'use strict';

const PROC_NM = 'land_file v1.0';

// later retrieved from current_warehouse(), etc
let WAREHOUSE = '', DATABASE = '', SCHEMA = '', USER = '', ROLE = '';


// log message to table - details might be the SQL about to be run or a stack trace
function log(msg, details=null) {
    snowflake.execute({
        sqlText: 'insert into audit_log(src_nm, user_nm, role_nm, wh_nm, msg_txt, extra_txt) values(:1, :2, :3, :4, :5, :6)',
        binds: [PROC_NM, USER, ROLE, WAREHOUSE, msg, details]
    });
}

// parse table name into [database.][schema.]table
function parse_name(label, nm, def_db, def_schema) {
    let database = def_db, schema   = def_schema, table= nm;
    const parts = nm.toUpperCase().split('.');

    if (parts.length === 3)
        [database, schema, table] = parts;
    else if (parts.length === 2)
        [schema, table] = parts;
    else if (parts.length === 1)
        table = parts[0];
    else if (parts.length === 0)
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

    const [tgt_db_nm, tgt_schema, tgt_tbl_nm] = parse_name('Target', TGT, DATABASE, SCHEMA);
    log('Target:', `${tgt_db_nm}.${tgt_schema}.${tgt_tbl_nm}`);

    const params = JSON.parse(PARAMS);
    log('Parameters:', JSON.stringify(params, null, 4));

    // set defaults
    // Bah! - can't use these till ES2021 gets ratified (June 2021)
    //params.format ??= 'parquet';
    //params.stage  ??= 'jk_db.landing.ba_sandbox_aligned_stage/ICW';
    params.format = params.format ?? 'parquet';
    params.stage  = params.stage  ?? 'jk_db.landing.ba_sandbox_aligned_stage/ICW';

    log('Parameters:', JSON.stringify(params, null, 4));


    // 1 of n: create/replace our base table
    let base_sql = `
        CREATE OR REPLACE TABLE ${tgt_db_nm}.${tgt_schema}.B_${tgt_tbl_nm} (
            SRC_FILE_NM     STRING,
            LOAD_TS         TIMESTAMP_NTZ,
            VALUE           VARIANT  )
    `;


    // 2 of n: create/replace pipe to automate the loads
    let pipe_sql = `
        CREATE OR REPLACE PIPE ${tgt_db_nm}.${tgt_schema}.P_${tgt_tbl_nm} auto_ingest=true AS
          COPY INTO ${tgt_db_nm}.${tgt_schema}.B_${tgt_tbl_nm} (src_file_nm, load_ts, value)
          FROM ( -- tag on filename and current time
                 SELECT metadata$filename, current_timestamp(), $1
                   FROM @${params.stage}/${SRC} )
          file_format = ( type = ${params.format} )
    `;



    // cover function to run and log some details
    function logAndRun(msg, sql) {
        log(msg, sql);
        let s = snowflake.createStatement( {sqlText: sql}  )
        let r = s.execute();
    }


    logAndRun('Generated table create:', base_sql);
    logAndRun('Generated pipe create:',  pipe_sql);

    return 'OK'

}

catch (e) {
    log(`ERROR: (${e.name}) ${e.message}`, e.stack);
    return `${e.message}`;
}
$$
;