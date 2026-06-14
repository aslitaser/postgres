-- xid time map static surface

SELECT proname, pg_get_function_arguments(oid), pg_get_function_result(oid)
FROM pg_proc
WHERE proname IN ('pg_xid_assigned_at', 'pg_xid_at_time')
ORDER BY proname;

SELECT pg_xid_assigned_at('1'::xid8) IS NULL AS very_old_xid_is_null;
SELECT pg_xid_assigned_at('18446744073709551615'::xid8) IS NULL AS future_xid_is_null;
SELECT pg_xid_at_time('2000-01-01 00:00:00+00'::timestamptz) IS NULL AS old_time_is_null;
