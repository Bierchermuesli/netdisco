#!/usr/bin/env perl

use strict; use warnings;

# The job timeout is Sys::SigAction's empty hashref. When it fired inside
# _try_connect, the catch there turned it into a string, and run_workers
# recorded that as one worker's error and ran the next workers with no
# timeout left. Each tried every device_auth stanza again, so a macsuck on a
# device that did not answer ran for an hour instead of workers.timeout and
# ended "(empty hashref - MCE worker killed or timed out)". The timeout must
# end the job, and the job log must say it timed out.

BEGIN { $ENV{DANCER_ENVDIR} = '/dev/null'; }

use Test::More 0.88;
use Test::File::ShareDir::Dist { 'App-Netdisco' => 'share/' };

use lib 'xt/lib';

use App::Netdisco;
use App::Netdisco::DB;
use App::Netdisco::Backend::Job;

use Dancer qw/:moose :script !pass/;

set logger => 'null';

{
  package MyWorker;
  use Moo;
  with 'App::Netdisco::Worker::Runner';
}

config->{'device_auth'} = [{driver => 'snmp'}];
config->{'extra_worker_plugins'} = ['X::TestNine'];
config->{'testnine_timeout'} = 1;

subtest 'run__timeout_in_snmp_connect__job_ends_timed_out' => sub {
  # a stored device: Runner adds 40s per device_auth stanza for a new one
  my $device = App::Netdisco::DB->resultset('Device')->new_result({ip => '192.0.2.1'});
  $device->in_storage(1);

  my $job = App::Netdisco::Backend::Job->new({
    job => 0,
    device => $device,
    action => 'testnine',
  });

  my $start = time;
  MyWorker->new()->run($job);

  is($job->status, 'error');
  like($job->log, qr/job timed out after 1 sec/, 'the log names the timeout');
  no warnings 'once';
  is($App::NetdiscoX::Worker::Plugin::TestNine::store_ran, 0,
    'no worker runs after the timeout');
  cmp_ok(time - $start, '<', 4, 'the job ends at the timeout');
};

done_testing;
