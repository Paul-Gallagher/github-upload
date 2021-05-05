create or replace function columntype_default( TYPE string )
    returns string
    language javascript
    comment = 'Used by temporal|normal_merge when generating COALESCE(x, default) calls'
    as
$$
'use strict';

switch (TYPE) {

    case 'TEXT':
        return "''";

    case 'DATE':
        return "'0001-01-01'::date";

    case 'TIME':
        return "'00:00:00'::time";

    case 'TIMESTAMP_NTZ':
        return "'0000-01-01'::timestamp";

    default:
        return '0';
}
$$
;
