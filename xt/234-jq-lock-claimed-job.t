#!/usr/bin/env perl

# Every manager reads jobs with jq_getsome and claims each with jq_lock.
# Managers that read the same job all reached the update in jq_lock, which
# matched on status alone: the first claimed it, and each later one waited
# for the row lock and claimed it again, so the job ran once per manager.
# High priority jobs sort first for every manager, so with six backends a
# hook::http job sent its request six times. A job another backend claimed
# must stay with it.
#
# No DANCER_ENVDIR=/dev/null here: that empties the DSN, and this needs a
# real database. admin is shadowed by a temporary table and rolled back.

use strict;
use warnings;

use Test::More 0.88;
use Test::File::ShareDir::Dist { 'App-Netdisco' => 'share/' };

use App::Netdisco;
use App::Netdisco::Backend::Job;
use App::Netdisco::JobQueue 'jq_lock';
use Dancer qw/:moose :script !pass/;
use Dancer::Plugin::DBIC 'schema';

config->{logger} = 'null';
# jq_lock reads schema(vars->{'tenant'}); the same connection must see the
# temporary table
var(tenant => 'netdisco');

my $schema = eval {
  my $s = schema('netdisco');
  $s->storage->dbh_do(sub { $_[1]->do('SELECT 1 FROM admin LIMIT 1') });
  $s;
};
my $why = $@;

# The skip reason is always printed: a silent skip would leave this file
# green while proving nothing.
SKIP: {
  skip "no usable netdisco database: $why", 1 if not $schema;

  my $dbh = $schema->storage->dbh;
  $schema->txn_begin;
  $dbh->do('SET LOCAL client_min_messages = warning');
  $dbh->do('CREATE TEMP TABLE admin (LIKE admin INCLUDING DEFAULTS)');
  $dbh->do(q{INSERT INTO pg_temp.admin (job, action, device, status)
             VALUES (1, 'discover', '192.0.2.1', 'queued')});

  my $job = App::Netdisco::Backend::Job->new({
    job => 1, action => 'discover', device => '192.0.2.1' });

  my $claim = sub {
    local setting('workers')->{'BACKEND'} = shift;
    return jq_lock($job) ? 1 : 0;
  };

  my $first  = $claim->('backend-a');
  my $second = $claim->('backend-b');
  my ($backend) = $dbh->selectrow_array('SELECT backend FROM pg_temp.admin WHERE job = 1');

  $schema->txn_rollback;

  subtest 'jqLock__job_claimed_by_another_backend__stays_with_it' => sub {
    is $first, 1, 'the first backend claims the queued job';
    is $second, 0, 'a second backend cannot claim it again';
    is $backend, 'backend-a', 'the job stays booked to the first backend';
  };
}

done_testing;
