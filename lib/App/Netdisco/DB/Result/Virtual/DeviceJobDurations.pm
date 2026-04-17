package App::Netdisco::DB::Result::Virtual::DeviceJobDurations;

use strict;
use warnings;

use base 'DBIx::Class::Core';

__PACKAGE__->table_class('DBIx::Class::ResultSource::View');

__PACKAGE__->table('device_job_durations');
__PACKAGE__->result_source_instance->is_virtual(1);
__PACKAGE__->result_source_instance->view_definition(<<'ENDSQL');
SELECT device, action, status, started, duration, rn
FROM (
  SELECT device,
         action,
         status,
         started,
         floor(extract(epoch FROM (finished - started))) AS duration,
         row_number() OVER (PARTITION BY device, action ORDER BY started DESC) AS rn
  FROM admin
  WHERE device  IS NOT NULL
    AND started  IS NOT NULL
    AND finished IS NOT NULL
) t
ENDSQL

__PACKAGE__->add_columns(
  'device'   => { data_type => 'inet',    is_nullable => 1 },
  'action'   => { data_type => 'text',    is_nullable => 1 },
  'status'   => { data_type => 'text',    is_nullable => 1 },
  'started'  => { data_type => 'timestamp', is_nullable => 1 },
  'duration' => { data_type => 'numeric', is_nullable => 1 },
  'rn'       => { data_type => 'integer', is_nullable => 1 },
);

1;
