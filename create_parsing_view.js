// Snowflake version of lander - create a view on top of a base table when we have some data
//
// TODO: add more info to this header
// sample call: call create_parsing_view('sky_searches', '{}');
//
// v1.0  PG     Apr 21  Initial version

create or replace procedure create_parsing_view (TBL     string,     // base table
                                                 PARAMS  string )    // params as JSON string (needs "s)
    returns string
    language javascript
    comment = 'Snowflake version of lander'
    as
$$
'use strict';

const PROC_NM = 'create_parsing_view v1.0';

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

    const [tbl_db_nm, tbl_schema, tbl_nm] = parse_name('Target table', TBL, DATABASE, SCHEMA);
    log('Target table:', `${tbl_db_nm}.${tbl_schema}.${tbl_nm}`);

    const params = JSON.parse(PARAMS);
    log('Parameters:', JSON.stringify(params, null, 4));

    // set defaults
    //params.maxDepth = params.maxDepth ?? 3;

    log('Parameters:', JSON.stringify(params, null, 4));


    // grab one row and hope it's representative
    let sql = `SELECT src_file_nm,
                      load_ts,
                      value
                 FROM ${tbl_db_nm}.${tbl_schema}.B_${tbl_nm}
                LIMIT 1`;
    log('Sampling query:', sql);

    let stmt = snowflake.createStatement( {sqlText: sql} )
    let results = stmt.execute();
    results.next();

    let fields = results.getColumnValue('VALUE');
    log('Fields:', JSON.stringify(fields, null, 4));


    // helper: convert names such as "leg[2]_cabinClass" to "leg_2_cabinClass"
    function normalise(fn) {
        const s = fn.replace(/"/g,'').replace(/\[(.+?)\](_?)/g, '_$1$2').trim();
        return s.replace(/([^a-z0-9_,])/ig, '_').padEnd(30);       // convert any other chars to _ to be safe
    }

    // helper: bung quotes around name, stick on prefix and pad to width - if index then include "[index]"
    function fixup(prefix, name, width, index=undefined) {
        let r = `${prefix}"${name}"`;
        if (index !== undefined) r+= `[${index}]`;
        return r.padEnd(width)
    }

    // build our view
    let view_sql = `
        CREATE OR REPLACE VIEW ${tbl_db_nm}.${tbl_schema}.V_${tbl_nm} AS (

        SELECT
              -- standard columns
              src_file_nm
            , load_ts
              -- parsed columns \n`;

    // iterate, possibly recursively, over the fields we have - add "" to deal with whacky names (like "leg[1]_cabinClass" say)
    function extractField(depth, prefix, fields) {
        const width = 50;
        for (const f in fields) {

            // deal with arrays
            if (Array.isArray(fields[f])) {
                view_sql += `              -- enumerating array ${f} ...\n`;
                view_sql += `            , value:${fixup(prefix,f,width)}::variant  as ${normalise(prefix+f)}   -- raw array \n`;
                for (var i = 0; i < fields[f].length; i++)
                    view_sql += `            , value:${fixup(prefix,f,width,i)}::string   as ${normalise(prefix+f+'['+i+']')}   -- ${typeof(fields[f][i])} \n`;
                view_sql += `\n`;
            }
            // deal with nested objects
            else if (typeof(fields[f]) === 'object') {
                if (depth < params.maxDepth) {
                    view_sql += `              -- recursing(${depth+1}) into ${f} \n`;
                    view_sql += `            , value:${fixup(prefix,f,width)}::variant  as ${normalise(prefix+f)}   -- raw object \n`;
                    extractField(depth+1, `${prefix}"${f}".`, fields[f]);
                } else {
                    view_sql += `              -- exceeded maxDepth(${params.maxDepth}) - can't recurse into ${f} \n`;
                    view_sql += `            , value:${fixup(prefix,f,width)}::variant  as ${normalise(prefix+f)}   -- raw object \n`;
                }
                view_sql += `\n`;
            }
            // normal fields
            else
                view_sql += `            , value:${fixup(prefix,f,width)}::string   as ${normalise(prefix+f)}   -- ${typeof(fields[f])} \n`;
          }
    }
    extractField(0, '', fields);

    // close off the statement
    view_sql += `
          FROM ${tbl_db_nm}.${tbl_schema}.B_${tbl_nm}
    )
    `;
    log('View sql:', view_sql);

    results = snowflake.createStatement( {sqlText: view_sql} ).execute();
    return 'OK'

}

catch (e) {
    log(`ERROR: (${e.name}) ${e.message}`, e.stack);
    return `${e.message}`;
}
$$
;