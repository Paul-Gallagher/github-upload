// Snowflake version of Paul Shade's temporal_merge
// TODO: add more info to this header
// sample call: call temporal_merge('t_test', 'b_test', 'KEY:[key1,key2,key3]; BUS:[bus_eff_ts,bus_exp_ts]', null);
//
// v1.0  PG     Apr 21  Initial version
//  "    PG  23 Apr 21  Allow PRIMARY KEY columns to be NULL (for Tony)

create or replace procedure temporal_merge( SRC     string,     // source [db.][schema.]tablename
                                            TGT     string,     // target [db.][schema.]tablename
                                            ROLES   string,     // column roles (KEY, BUS, SYS, OPR)
                                            DRIVER  string )    // optional driver [db.][schema.]tablename
    returns string
    language javascript
    comment = 'Snowflake version of Paul Shade''s temporal_merge'
    execute as owner   // caller or owner (the default)
    as
$$
'use strict';

const PROC_NM = 'temporal_merge v1.0';

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

    const [src_db_nm, src_schema, src_tbl_nm] = parse_name('Source', SRC,                    DATABASE, SCHEMA);
    const [tgt_db_nm, tgt_schema, tgt_tbl_nm] = parse_name('Target', TGT,                    DATABASE, SCHEMA);
    const [dvr_db_nm, dvr_schema, dvr_tbl_nm] = parse_name('Driver', !DRIVER ? SRC : DRIVER, DATABASE, SCHEMA);
    log('Parameters:', `Source: ${src_db_nm}.${src_schema}.${src_tbl_nm}, Target: ${tgt_db_nm}.${tgt_schema}.${tgt_tbl_nm}`);
    (!DRIVER) ? log('No master/driver') : ('Master/driver:', `${dvr_db_nm}.${dvr_schema}.${dvr_tbl_nm}`);


    // dictionary query - constructed from a Javascript template string (like a Python f-string)
    const sql = `
        SELECT coalesce(src.column_name, '')                                                  as src_col_nm,
               tgt.column_name                                                                as tgt_col_nm,
               CASE when rol.role_name is not null                       THEN rol.role_name
                    when tgt.column_name in ('ICW_EFF_TS', 'ICW_EXP_TS') THEN 'SYS'
                                                                         ELSE 'HIS'
               END                                                                            as column_role,
               columntype_default(tgt.data_type)                                              as column_default

            -- not used in this version ------------------------------------------------------------------------
            --
            -- tgt.data_type                                                                  as raw_type,
            -- tgt.character_maximum_length                                                   as raw_length,
            -- tgt.numeric_precision                                                          as raw_precision,
            -- tgt.numeric_scale                                                              as raw_scale,
            -- tgt.datetime_precision                                                         as raw_time_precision,
            -- column_type_to_txt(tgt.data_type,
            --                    tgt.character_maximum_length,
            --                    coalesce(tgt.numeric_precision, tgt.datetime_precision),
            --                    tgt.numeric_scale)                                          as column_type
            -----------------------------------------------------------------------------------------------------

          FROM ( -- source table)
                  SELECT table_name,
                         ordinal_position,
                         column_name,
                         data_type,
                         is_nullable,
                         character_maximum_length,
                         numeric_precision,
                         numeric_scale,
                         datetime_precision,
                         is_identity
                    FROM ${src_db_nm}.information_schema.columns s
                   WHERE table_catalog = '${src_db_nm}'
                     AND table_schema  = '${src_schema}'
                     AND table_name    = '${src_tbl_nm}'
                ) src

      FULL JOIN ( -- target table
                  SELECT table_name,
                         ordinal_position,
                         column_name,
                         data_type,
                         is_nullable,
                         character_maximum_length,
                         numeric_precision,
                         numeric_scale,
                         datetime_precision,
                         is_identity
                    FROM ${tgt_db_nm}.information_schema.columns s
                   WHERE table_catalog = '${tgt_db_nm}'
                     AND table_schema  = '${tgt_schema}'
                     AND table_name    = '${tgt_tbl_nm}'
                ) tgt

             ON src.column_name = tgt.column_name

      LEFT JOIN ( -- Roles are given in the form:  label1: value,value... [; label2:value, value...]
                  --   eg KEY:col1,col2; BUS:bus_eff_ts, bus_exp_ts; SYS:icw_eff_ts, icw_exp_ts
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

    const kem_list = [];
    const key_list = [];
    const his_list = [];
    const opr_list = [];
    const bus_list = [];
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

        // more or less replicated from Teradata versions  (with less farting around)
        if (tgt_col_nm === null && column_role !== 'OPR' )
            log(`WARNING: Source column ${src_col_nm} not found on target table`);

        if (src_col_nm === null )
            throw new ReferenceError(`Required column ${tgt_col_nm} not found on source table`);

        if (column_role === 'KEM')
            kem_list.push(tgt_col_nm);

        if (column_role === 'KEM' || column_role === 'KEY') {
            key_list.push(tgt_col_nm);
            key_defl.push(column_default)
        }
        else if (column_role === 'HIS') {
            his_list.push(tgt_col_nm);
            his_defl.push(column_default)
        }
        else if (column_role === 'OPR')
            opr_list.push(src_col_nm);

        else if (column_role === 'UPD')
            throw new ReferenceError(`The UPD role is no longer supported`);

        // have to cater for Snowflake not supporting period types - we'll have two BUS and two SYS columns
        else if (column_role === 'BUS')
            bus_list.push(tgt_col_nm);

        else if (column_role === 'SYS')
            sys_list.push(tgt_col_nm);

        else
            oth_list.push(column_role);
    }

    // Sanity checks after Tony's first run - no BUS or SYS found - in fact no columns found in target
    if (oth_list.length   > 0) throw new ReferenceError(`Unknown role(s) detected - ${oth_list.filter((v, i, s) => s.indexOf(v) === i)}`);  // unique   TODO: use a Set
    if (key_list.length === 0) throw new ReferenceError(`Required KEY field(s not found in target`);
    if (bus_list.length !== 2) throw new ReferenceError(`Required BUS field(s) not found in target`);
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



    // 1 of 5: input_set sql  (determines if there is historical data with ranges that needs to be inserted within existing ranges (sic))
    let input_sql = `
        CREATE OR REPLACE TEMPORARY TABLE input_set AS (

        SELECT -- key columns
               ${prefixAndIndent(key_list, '',  17)},
               -- history columns
               ${prefixAndIndent(his_list, '',  17)},
               -- business effective columns
               ${prefixAndIndent(bus_list, '',  17)},

               prog_flag,

               -- adjustments for overlapping ranges (apparently)
               LAG(${bus_list[1]})  OVER (
                                            PARTITION BY ${key_list.join(', ')}
                                                ORDER BY ${bus_list.join(', ')}
                                         )                                        as orig_prev_end,
               LEAD(${bus_list[0]}) OVER (
                                            PARTITION BY ${key_list.join(', ')}
                                                ORDER BY ${bus_list.join(', ')}
                                         )                                        as orig_next_start,
               CASE
                   WHEN orig_prev_end >= '9999-12-31'::timestamp
                   THEN ${bus_list[0]}
                   ELSE orig_prev_end
               END                                                                as prev_end,
               CASE
                   WHEN orig_next_start < ${bus_list[1]}
                    AND ${bus_list[1]}  < '9999-12-31'::timestamp
                   THEN ${bus_list[1]}
                   ELSE orig_next_start
               END                                                                as next_start

          FROM ( -- incoming records (Teradata uses normalize on meets or overlaps here)
                 SELECT ${prefixAndIndent(key_list, '',  26)},
                        ${prefixAndIndent(his_list, '',  26)},
                        ${prefixAndIndent(bus_list, '',  26)},
                        0                                         as prog_flag

                   FROM ${src_db_nm}.${src_schema}.${src_tbl_nm} inp
    `;

    // 1.1 of 3: possibly deal with logically deleted records (role OPR)
    if (opr_list.length !== 0) {

        input_sql += `
                  WHERE coalesce(${opr_list[0]}, '') <> 'E'

                  UNION

                  -- target records to expire
                 SELECT ${prefixAndIndent(key_list, 'tgt.',  26)},
                        ${prefixAndIndent(his_list, 'tgt.',  26)},
                        ${prefixAndIndent(bus_list, 'tgt.',  26)},
                        1                                         as prog_flag

                   FROM ${src_db_nm}.${src_schema}.${src_tbl_nm} inp

                   -- join source and target on (colaesced) key columns
                   JOIN ${tgt_db_nm}.${tgt_schema}.${tgt_tbl_nm} tgt
                     ON ${compareEach(key_list, key_defl, 'inp.', 'tgt.', 21)}

                  WHERE tgt.${sys_list[1]} > CURRENT_TIMESTAMP       -- current target records only
                    AND src.${opr_list[0]} = 'E'                     -- where source OPR column asks for expiry

                    -- only expire where target range contains expiry time from source
                    AND inp.${bus_list[0]} >= tgt.${bus_list[0]}
                    AND inp.${bus_list[0]}  < tgt.${bus_list[1]}
        `;
    }

    // 1.2 of 3: possibly deal with Key Master (KEM) details
    if (kem_list.length !== 0) {

        input_sql += `
                  UNION

                 -- Key Master (KEM) - round up records to expire
                 SELECT ${prefixAndIndent(key_list, 'tgt.',  26)},
                        ${prefixAndIndent(his_list, 'tgt.',  26)},
                        ${prefixAndIndent(bus_list, 'inph.', 26)},
                        1                                         as prog_flag

                   FROM ${tgt_db_nm}.${tgt_schema}.${tgt_tbl_nm} tgt

                   JOIN ( -- distinct master-keys & dates from master/driver
                          SELECT ${prefixAndIndent(kem_list, '',  34)},
                                 ${prefixAndIndent(bus_list, '',  34)}
                            FROM ${dvr_db_nm}.${dvr_schema}.${dvr_tbl_nm}
                        GROUP BY ${prefixAndIndent(kem_list, '',  34)},
                                 ${prefixAndIndent(bus_list, '',  34)}
                        ) inph
                     ON ${compareEach(kem_list, key_defl, 'inph.', 'tgt.', 21)}

                    -- Teradata version uses tgt period contains inph period
                    AND inph.${bus_list[0]} BETWEEN tgt.${bus_list[0]} AND tgt.${bus_list[1]}
                    AND inph.${bus_list[1]} BETWEEN tgt.${bus_list[0]} AND tgt.${bus_list[1]}

              LEFT JOIN ( -- distinct keys and dates from source
                          SELECT ${prefixAndIndent(key_list, '',  34)},
                                 ${prefixAndIndent(bus_list, '',  34)}
                            FROM ${src_db_nm}.${src_schema}.${src_tbl_nm}
                        GROUP BY ${prefixAndIndent(key_list, '',  34)},
                                 ${prefixAndIndent(bus_list, '',  34)}
                        ) inp
                     ON ${compareEach(key_list, key_defl, 'inp.', 'tgt.', 21)}

                    AND ${bus_list.map(e => 'inph.' + e + ' = inp.' + e).join('\n' + ' '.repeat(21) + 'AND ')}

                   WHERE tgt.${sys_list[1]} > CURRENT_TIMESTAMP       -- current target records only
                     AND inp.${key_list[0]} IS NULL                   -- no matching row found in inp table
        `;
    }

    // 1.3 of 3: close off the statement
    input_sql += `
               )
           )`;


    // 2 of 5: change_set sql  (resultant data periods based on incoming data) - added statement_no for debugging data
    let change_sql = `
        CREATE OR REPLACE TEMPORARY TABLE change_set AS (

        -- 1. all records from input_set
        SELECT -- key columns
               ${prefixAndIndent(key_list, '',  17)},
               -- history columns
               ${prefixAndIndent(his_list, '',  17)},
               -- business effective columns
               ${prefixAndIndent(bus_list, '',  17)},

               prog_flag,
               1                                                  as statement_no

          FROM input_set

         UNION

        -- 2. records from target that are BEFORE input records
        SELECT -- key columns
               ${prefixAndIndent(key_list, 'tgt.',  17)},
               -- history columns
               ${prefixAndIndent(his_list, 'tgt.',  17)},
               -- business effective columns -Teradata version uses periods and tgt ldiff inp (portion of 1st period before overlapping 2nd period)
               CASE
                   WHEN inp.prev_end > tgt.${bus_list[0]}
                   THEN inp.prev_end
                   ELSE tgt.${bus_list[0]}  -- ldiff start
               END                                                as ${bus_list[0]},
               CASE
                   WHEN inp.prev_end > tgt.${bus_list[0]}
                   THEN inp.${bus_list[0]}
                   ELSE inp.${bus_list[0]}  -- ldiff end
               END                                                as ${bus_list[1]},
               0                                                  as prog_flag,
               2                                                  as statement_no

          FROM input_set inp

          JOIN ${tgt_db_nm}.${tgt_schema}.${tgt_tbl_nm} tgt
            ON ${compareEach(key_list, key_defl, 'tgt.', 'inp.', 11)}

           AND tgt.${sys_list[1]} > CURRENT_TIMESTAMP       -- current target records only

           -- Teradata version uses tgt ldiff inp is not null
           AND tgt.${bus_list[0]} < inp.${bus_list[1]}  -- overlaps: (StartA < EndB)  and  (EndA > StartB)
           AND tgt.${bus_list[1]} > inp.${bus_list[0]}

           AND tgt.${bus_list[0]} < inp.${bus_list[0]}  -- ldiff: Start1 < Start2

           AND (    inp.prev_end IS NULL
                 OR inp.prev_end < inp.${bus_list[0]} )

         UNION

        -- 3. records from target that are AFTER input records
        SELECT -- key columns
               ${prefixAndIndent(key_list, 'tgt.',  17)},
               -- history columns
               ${prefixAndIndent(his_list, 'tgt.',  17)},
               -- business effective columns - Teradata version uses periods and tgt rdiff inp (portion of 1st period after overlapping 2nd period)
               CASE
                   WHEN inp.next_start < tgt.${bus_list[1]}
                   THEN inp.${bus_list[1]}
                   ELSE inp.${bus_list[1]}  -- rdiff start
               END                                                as ${bus_list[0]},
               CASE
                   WHEN inp.next_start < tgt.${bus_list[1]}
                   THEN inp.next_start
                   ELSE tgt.${bus_list[1]}  -- rdiff end
               END                                                as ${bus_list[1]},
               -- set to 1 if we have a KEM column - forces expiry of all target ranges after input range (in mysterious ways)
               ${kem_list.length === 0 ? 0 : 1}                   as prog_flag,
               3                                                  as statement_no

          FROM input_set inp

          JOIN ${tgt_db_nm}.${tgt_schema}.${tgt_tbl_nm} tgt
            ON ${compareEach(key_list, key_defl, 'tgt.', 'inp.', 11)}

           AND tgt.${sys_list[1]} > CURRENT_TIMESTAMP       -- current target records only

           -- Teradata version uses tgt rdiff inp is not null
           AND tgt.${bus_list[0]} < inp.${bus_list[1]}  -- overlaps: (StartA < EndB)  and  (EndA > StartB)
           AND tgt.${bus_list[1]} > inp.${bus_list[0]}

           AND tgt.${bus_list[1]} > inp.${bus_list[1]}  -- rdiff: End1 > End2

           AND (    inp.next_start IS NULL
                 OR inp.next_start <> inp.${bus_list[1]} )

         UNION

        -- 4. records from target that overlap input records
        SELECT -- key columns
               ${prefixAndIndent(key_list, 'tgt.',  17)},
               -- history columns
               ${prefixAndIndent(his_list, 'tgt.',  17)},
               -- business effective columns
               ${prefixAndIndent(bus_list, 'tgt.',  17)},
               0                                                  as prog_flag,
               4                                                  as statement_no

          FROM ${tgt_db_nm}.${tgt_schema}.${tgt_tbl_nm} tgt

          JOIN ( -- Teradata version uses prior() to get 'one granule less'
                 SELECT ${prefixAndIndent(key_list, '',  26)},
                        min(${bus_list[0]})          as thestart,
                        max(${bus_list[1]})          as theend
                   FROM input_set
               GROUP BY ${prefixAndIndent(key_list, '',  26)}
               ) rng
            ON ${compareEach(key_list, key_defl, 'rng.', 'tgt.', 11)}

           -- Teradata version (incorrectly) has tgt contains rng.thestart OR tgt contains rng.theend
           AND (    (     rng.thestart >= tgt.${bus_list[0]}
                      AND rng.thestart <  tgt.${bus_list[1]}
                    )
                 OR (     rng.theend   >  tgt.${bus_list[0]}  -- end of a range is non-inclusive
                      AND rng.theend   <  tgt.${bus_list[1]}
                    )
                )

     LEFT JOIN input_set inp
            ON ${compareEach(key_list, key_defl, 'inp.', 'tgt.', 11)}

           -- Teradata uses inp p_intersect tgt is not null   (common portion of overlapping periods)
           AND tgt.${bus_list[0]} < inp.${bus_list[1]}      -- overlaps: (StartA < EndB)  and  (EndA > StartB)

         WHERE tgt.${sys_list[1]} > CURRENT_TIMESTAMP       -- current target records only
           AND inp.${key_list[0]} IS NULL                   -- don't already have this on inp table

           )`;


    // 3 of 5: normal_set sql  (normalises the data) - Teradata uses normalize_overlap_meet - this takes a different path :-)   (maybe rewrite as table function sometime?)
    let normal_sql = `
        CREATE OR REPLACE TEMPORARY TABLE normal_set AS (

        WITH flagged as ( -- mark starts of overlapping or meeting ranges (with same keys and details)

               SELECT -- key columns
                      ${prefixAndIndent(key_list, '',  24)},
                      -- history columns
                      ${prefixAndIndent(his_list, '',  24)},
                      -- business effective columns
                      ${prefixAndIndent(bus_list, '',  24)},
                      prog_flag,
                      CASE
                          WHEN ${bus_list[0]} <= LAG(${bus_list[1]}) OVER (
                              PARTITION BY ${prefixAndIndent(key_list, '',  45)},
                                           ${prefixAndIndent(his_list, '',  45)}
                                  ORDER BY ${bus_list.join(', ')} )
                          THEN 0 ELSE 1
                      END                       AS group_flag

                 FROM change_set
          ),

          grouped AS ( -- cumulatively sum the flags to give us group_ids

              SELECT *,
                     SUM(group_flag) OVER ( PARTITION BY ${prefixAndIndent(key_list, '',  59)},
                                                         ${prefixAndIndent(his_list, '',  59)},
                                                         prog_flag
                                                ORDER BY ${bus_list[0]}
                                            ROWS UNBOUNDED PRECEDING ) AS group_id

                FROM flagged
          )

        -- find the min/max range for each of our group ids
        SELECT -- key columns
               ${prefixAndIndent(key_list, '',  17)},
               -- history columns
               ${prefixAndIndent(his_list, '',  17)},
               -- adjusted business effective columns
               MIN(${bus_list[0]})                        as ${bus_list[0]},
               MAX(${bus_list[1]})                        as ${bus_list[1]},
               prog_flag

          FROM grouped t2

      GROUP BY ${prefixAndIndent(key_list, '',  17)},
               ${prefixAndIndent(his_list, '',  17)},
               prog_flag,
               group_id

           )`;


    // 4 of 5: merge_set - final data to be used for merging into target
    let merge_sql = `
        CREATE OR REPLACE TEMPORARY TABLE merge_set AS (

        -- all records from normal_set (with prog_flag=0) and system generated effectivity
        SELECT -- key columns
               ${prefixAndIndent(key_list, '',  17)},
               -- history columns
               ${prefixAndIndent(his_list, '',  17)},
               -- business effective columns
               ${prefixAndIndent(bus_list, '',  17)},
               -- system generated effectivity
               CURRENT_TIMESTAMP::timestamp_ntz             as ${sys_list[0]},
               '9999-12-31 23:59:59.999'::timestamp_ntz     as ${sys_list[1]}

          FROM normal_set
         WHERE prog_flag = 0

         UNION

        -- expire records that have changed
        SELECT -- key columns
               ${prefixAndIndent(key_list, 'tgt.',  17)},
               -- history columns
               ${prefixAndIndent(his_list, 'tgt.',  17)},
               -- business effective columns
               ${prefixAndIndent(bus_list, 'tgt.',  17)},
               -- existing effective, system generated expiry
               tgt.${sys_list[0]}::timestamp_ntz,
               CURRENT_TIMESTAMP::timestamp_ntz             as ${sys_list[1]}

          FROM ${tgt_db_nm}.${tgt_schema}.${tgt_tbl_nm} tgt

          JOIN ( -- min/max times for keys of changed records
                 SELECT ${prefixAndIndent(key_list, '',  26)},
                        min(${bus_list[0]})     as ${bus_list[0]},
                        max(${bus_list[1]})     as ${bus_list[1]}
                   FROM normal_set
               GROUP BY ${prefixAndIndent(key_list, '',  26)}
               ) rng
            ON ${compareEach(key_list, key_defl, 'rng.', 'tgt.', 11)}

           -- Teradata uses rng's min-max-period contains tgt.bus_eff_pd
           AND rng.${bus_list[0]} <= tgt.${bus_list[0]}
           AND rng.${bus_list[1]} >= tgt.${bus_list[1]}

     LEFT JOIN normal_set nor
            ON ${compareEach(key_list, key_defl, 'nor.', 'tgt.', 11)}

           AND ${bus_list.map(e => 'nor.' + e + ' = tgt.' + e).join('\n' + ' '.repeat(11) + 'AND ')}

           -- history columns
           AND ${compareEach(his_list, his_defl, 'nor.', 'tgt.', 11)}

         WHERE tgt.${sys_list[1]} > CURRENT_TIMESTAMP       -- current target records only
           AND nor.${key_list[0]} IS NULL                   -- no matching row found in nor table

         UNION

        -- expire records which match input driver (sic)
        SELECT -- key columns
               ${prefixAndIndent(key_list, 'tgt.',  17)},
               -- history columns
               ${prefixAndIndent(his_list, 'tgt.',  17)},
               -- business effective columns
               ${prefixAndIndent(bus_list, 'tgt.',  17)},
               -- existing effective, system generated expiry
               tgt.${sys_list[0]}::timestamp_ntz,
               CURRENT_TIMESTAMP::timestamp_ntz             as ${sys_list[1]}

          FROM ${tgt_db_nm}.${tgt_schema}.${tgt_tbl_nm} tgt

          JOIN normal_set nor
            ON ${compareEach(key_list, key_defl, 'nor.', 'tgt.', 11)}

           AND ${bus_list.map(e => 'nor.' + e + ' = tgt.' + e).join('\n' + ' '.repeat(11) + 'AND ')}
           AND nor.prog_flag = 1

           -- history columns
           AND ${compareEach(his_list, his_defl, 'nor.', 'tgt.', 11)}

         WHERE tgt.${sys_list[1]} > CURRENT_TIMESTAMP       -- current target records only

           )`;


    // 5 of 5: final merge
    let final_sql = `
        MERGE INTO ${tgt_db_nm}.${tgt_schema}.${tgt_tbl_nm} tgt

             USING merge_set mer
                -- key columns
                ON ${compareEach(key_list, key_defl, 'tgt.', 'mer.', 15)}

               AND ${bus_list.map(e => 'tgt.' + e + ' = mer.' + e).join('\n' + ' '.repeat(15) + 'AND ')}

               -- history columns
               AND ${compareEach(his_list, his_defl, 'tgt.', 'mer.', 15)}

               AND tgt.${sys_list[1]} > CURRENT_TIMESTAMP       -- current target records only


        WHEN MATCHED AND tgt.${sys_list[1]} <> mer.${sys_list[1]} THEN UPDATE
            SET ${sys_list[1]} = mer.${sys_list[1]}


        WHEN NOT MATCHED THEN INSERT (
            -- key columns
            ${prefixAndIndent(key_list, '',  14)},
            -- history columns
            ${prefixAndIndent(his_list, '',  14)},
            -- effectivity columns
            ${prefixAndIndent(bus_list, '',  14)},
            ${prefixAndIndent(sys_list, '',  14)}
        )
        VALUES (
            -- key columns
            ${prefixAndIndent(key_list, 'mer.',  14)},
            -- history columns
            ${prefixAndIndent(his_list, 'mer.',  14)},
            -- effectivity columns
            ${prefixAndIndent(bus_list, 'mer.',  14)},
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
    logAndRun('Generated normal_set sql', normal_sql);
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