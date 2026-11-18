package App::NetdiscoX::Worker::Plugin::TestNine;

use Dancer ':syntax';
use App::Netdisco::Worker::Plugin;
use aliased 'App::Netdisco::Worker::Status';
use App::Netdisco::Transport::SNMP;

# an SNMP::Info class whose constructor hangs, as a device that never
# answers does, so the job timeout fires inside _try_connect
{
  package App::NetdiscoX::Worker::Plugin::TestNine::SlowInfo;
  sub new { sleep 5; return undef }
  $INC{'App/NetdiscoX/Worker/Plugin/TestNine/SlowInfo.pm'} = __FILE__;
}

our $store_ran = 0;

register_worker({ phase => 'main', driver => 'direct', title => 'connect hangs' }, sub {
  my ($job, $workerconf) = @_;
  App::Netdisco::Transport::SNMP::_try_connect(
    $job->device, 'App::NetdiscoX::Worker::Plugin::TestNine::SlowInfo',
    { read => 1, community => 'public' }, 'read',
    { DestHost => '192.0.2.1', RemotePort => 161, Version => 2,
      Timeout => 1_000_000, Retries => 0 }, 0);
  return Status->done('connected');
});

register_worker({ phase => 'store', driver => 'direct', title => 'after the timeout' }, sub {
  $store_ran = 1;
  return Status->done('stored');
});

true;
