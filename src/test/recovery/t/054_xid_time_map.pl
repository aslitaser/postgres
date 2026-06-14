# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use Test::More;
use Time::HiRes qw(usleep);

sub trim
{
	my ($str) = @_;

	chomp($str);
	return $str;
}

sub xid_delta
{
	my ($a, $b) = @_;

	return abs($a - $b);
}

sub split_row
{
	my ($row) = @_;

	return map { trim($_) } split /\|/, $row;
}

my $lag_seconds = 3;

my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1);
$node_primary->append_conf(
	'postgresql.conf', qq[
xid_time_map_interval = '1s'
]);
$node_primary->start;

my $backup_name = 'xid_time_map_backup';
$node_primary->backup($backup_name);

my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_standby->append_conf(
	'postgresql.conf', qq[
hot_standby_feedback = on
wal_receiver_status_interval = '1s'
]);
$node_standby->start;

ok( $node_primary->poll_query_until(
		'postgres',
		"SELECT count(*) = 1 FROM pg_stat_replication WHERE state = 'streaming'"
	),
	'standby is streaming');

my @sampled_xids;
for my $i (1 .. 4)
{
	push @sampled_xids,
	  trim($node_primary->safe_psql('postgres',
			"SELECT pg_current_xact_id()"));
	usleep(1_200_000);
}

my ($cur_xid, $xid_at_now) = split_row(
	$node_primary->safe_psql(
		'postgres', q[
WITH cur AS (SELECT pg_current_xact_id() AS xid)
SELECT xid::text, pg_xid_at_time(now())::text FROM cur;
]));

ok($xid_at_now >= $cur_xid - 5,
	'pg_xid_at_time(now()) returns a xid near the current xid');

my $round_trip_xid = trim(
	$node_primary->safe_psql(
		'postgres',
		"SELECT pg_xid_at_time(pg_xid_assigned_at('$sampled_xids[1]'::xid8))::text"
	));
ok(xid_delta($round_trip_xid, $sampled_xids[1]) <= 5,
	'round trip through time and xid stays within a few xids');

is( $node_primary->safe_psql(
		'postgres',
		"SELECT pg_xid_assigned_at('$sampled_xids[1]'::xid8) <= pg_xid_assigned_at('$sampled_xids[3]'::xid8)"
	),
	't',
	'pg_xid_assigned_at is monotonic for increasing xids');

$node_primary->wait_for_replay_catchup($node_standby);

my $standby_session =
  $node_standby->background_psql('postgres', on_error_stop => 1);
$standby_session->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
my $standby_pid =
  trim($standby_session->query_safe('SELECT pg_backend_pid()'));
$standby_session->query_safe('SELECT count(*) FROM pg_class');

ok( $node_standby->poll_query_until(
		'postgres',
		"SELECT backend_xmin IS NOT NULL FROM pg_stat_activity WHERE pid = $standby_pid"
	),
	'standby transaction has a pinned xmin');

my $pinned_xmin = trim(
	$node_standby->safe_psql(
		'postgres',
		"SELECT backend_xmin::text FROM pg_stat_activity WHERE pid = $standby_pid"
	));

ok( $node_primary->poll_query_until(
		'postgres',
		"SELECT backend_xmin::text = '$pinned_xmin' FROM pg_stat_replication"
	),
	'primary receives unclamped standby feedback');

ok( $node_primary->poll_query_until(
		'postgres', qq[
WITH activity AS (SELECT pg_current_xact_id()),
feedback AS (
	SELECT backend_xmin, pg_xid_assigned_at(backend_xmin::text::xid8) AS assigned_at
	FROM pg_stat_replication
)
SELECT count(*) = 1
	AND bool_and(backend_xmin::text = '$pinned_xmin'
		AND assigned_at IS NOT NULL
		AND EXTRACT(epoch FROM now() - assigned_at) > ($lag_seconds + 2))
FROM feedback, activity;
]),
	'unclamped standby feedback can hold an old xmin');

my ($baseline_xmin, $baseline_age) = split_row(
	$node_primary->safe_psql(
		'postgres', q[
SELECT backend_xmin::text,
	EXTRACT(epoch FROM now() - pg_xid_assigned_at(backend_xmin::text::xid8))::numeric(10,3)
FROM pg_stat_replication;
]));

my $log_start = -s $node_primary->logfile;
$node_primary->safe_psql('postgres',
	"ALTER SYSTEM SET max_standby_feedback_lag = '${lag_seconds}s'");
$node_primary->safe_psql('postgres', 'SELECT pg_reload_conf()');

ok( $node_primary->poll_query_until(
		'postgres', qq[
WITH activity AS (SELECT pg_current_xact_id()),
feedback AS (
	SELECT backend_xmin, pg_xid_assigned_at(backend_xmin::text::xid8) AS assigned_at
	FROM pg_stat_replication
)
SELECT count(*) = 1
	AND bool_and(backend_xmin::text::xid8 > '$pinned_xmin'::xid8
		AND assigned_at IS NOT NULL
		AND EXTRACT(epoch FROM now() - assigned_at) >= $lag_seconds
		AND EXTRACT(epoch FROM now() - assigned_at) <= ($lag_seconds + 6))
FROM feedback, activity;
]),
	'max_standby_feedback_lag advances feedback xmin to about the lag bound');

ok($node_primary->log_contains('hot standby feedback xmin clamped', $log_start),
	'primary logs standby feedback clamp');

my ($clamped_xmin, $clamped_age) = split_row(
	$node_primary->safe_psql(
		'postgres', q[
SELECT backend_xmin::text,
	EXTRACT(epoch FROM now() - pg_xid_assigned_at(backend_xmin::text::xid8))::numeric(10,3)
FROM pg_stat_replication;
]));

ok($clamped_xmin > $pinned_xmin,
	'clamped backend_xmin is newer than the standby pinned xmin');
ok($clamped_age >= $lag_seconds && $clamped_age <= $lag_seconds + 6,
	'clamped backend_xmin maps to roughly the configured lag');

$node_primary->safe_psql('postgres',
	'ALTER SYSTEM SET max_standby_feedback_lag = 0');
$node_primary->safe_psql('postgres', 'SELECT pg_reload_conf()');

ok( $node_primary->poll_query_until(
		'postgres', qq[
WITH activity AS (SELECT pg_current_xact_id())
SELECT backend_xmin::text = '$pinned_xmin'
FROM pg_stat_replication, activity;
]),
	'max_standby_feedback_lag = 0 restores unclamped standby feedback');

my $fallback_xmin = trim(
	$node_primary->safe_psql('postgres',
		'SELECT backend_xmin::text FROM pg_stat_replication'));
is($fallback_xmin, $pinned_xmin,
	'backend_xmin falls back to the standby pinned xmin');

$standby_session->query_safe('ROLLBACK');
$standby_session->quit;

diag "baseline backend_xmin=$baseline_xmin age=${baseline_age}s";
diag "clamped backend_xmin=$clamped_xmin age=${clamped_age}s";

done_testing();
