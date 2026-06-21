package App::Netdisco::Web::Metrics;

use Dancer ':syntax';
use Dancer::Plugin::DBIC;

use App::Netdisco::Util::Permission 'acl_matches';
use POSIX 'floor';
use Try::Tiny;

sub _header {
  my ($name, $help) = @_;
  return sprintf("# HELP %s %s\n# TYPE %s gauge\n", $name, $help, $name);
}

sub _sample {
  my ($name, $value, %labels) = @_;
  my $label_str = '';
  if (%labels) {
    $label_str = '{'. join(',', map { qq($_="$labels{$_}") } sort keys %labels) .'}';
  }
  return sprintf("%s%s %s\n", $name, $label_str, $value // 0);
}

if (my $metrics_path = setting('metrics_path')) {
get $metrics_path => sub {
  # Optional IP range restriction
  my $allow = setting('metrics_allow');
  if ($allow and ref $allow eq ref []) {
    my $remote = request->remote_address;
    unless (acl_matches($remote, $allow)) {
      status 403;
      return 'Forbidden';
    }
  }

  # Optional bearer token auth
  my $token = setting('metrics_token');
  if ($token) {
    my $auth = request->header('Authorization') // '';
    unless ($auth eq "Bearer $token") {
      status 401;
      header 'WWW-Authenticate' => 'Bearer realm="netdisco metrics"';
      return 'Unauthorized';
    }
  }

  content_type 'text/plain; version=0.0.4; charset=utf-8';

  my @tenants = ('netdisco');
  if (my $tdbs = setting('tenant_databases')) {
    push @tenants, map { $_->{'tag'} } @$tdbs;
  }

  my $output = '';

  # -- Device label helper (for all per-device metrics) ---------------------
  my $label_by = setting('metrics_device_label') // 'ip';
  my %dns_maps;
  if ($label_by eq 'dns') {
    foreach my $tenant (@tenants) {
      my %map = try {
        map { $_->{ip} => ($_->{dns} // $_->{ip}) }
          schema($tenant)->resultset('Device')->search({}, {
            select => ['ip', 'dns'], as => [qw/ip dns/],
          })->hri->all;
      } catch { () };
      $dns_maps{$tenant} = \%map;
    }
  }
  my $dev_label = sub {
    my ($tenant, $ip) = @_;
    return ($label_by eq 'dns') ? ($dns_maps{$tenant}{$ip} // $ip) : $ip;
  };

  # -- Statistics metrics (one row per tenant) -------------------------------
  my @stat_metrics = (
    [ netdisco_devices        => 'device_count',        'Total number of discovered devices' ],
    [ netdisco_device_ips     => 'device_ip_count',     'Total number of device IP addresses' ],
    [ netdisco_device_links   => 'device_link_count',   'Total number of layer2 links between devices' ],
    [ netdisco_device_ports   => 'device_port_count',   'Total number of device ports' ],
    [ netdisco_device_ports_up => 'device_port_up_count','Total number of device ports with up/up status' ],
    [ netdisco_ip_table       => 'ip_table_count',      'Total number of IP table entries' ],
    [ netdisco_ip_active      => 'ip_active_count',     'Total number of active IP entries' ],
    [ netdisco_nodes          => 'node_table_count',    'Total number of node entries' ],
    [ netdisco_nodes_active   => 'node_active_count',   'Total number of active nodes' ],
    [ netdisco_phones         => 'phone_count',         'Total number of discovered VoIP phones' ],
    [ netdisco_waps           => 'wap_count',           'Total number of discovered wireless access points' ],
  );

  foreach my $m (@stat_metrics) {
    my ($metric, $col, $help) = @$m;
    $output .= _header($metric, $help);
    foreach my $tenant (@tenants) {
      my $stats = try {
        schema($tenant)->resultset('Statistics')
          ->search(undef, { order_by => { -desc => 'day' }, rows => 1 })->first;
      };
      next unless $stats;
      $output .= _sample($metric, $stats->$col // 0, tenant => $tenant);
    }
    $output .= "\n";
  }

  # Age of latest statistics row in seconds
  $output .= _header('netdisco_stats_age_seconds', 'Age of the latest statistics snapshot in seconds');
  foreach my $tenant (@tenants) {
    my $age = try {
      schema($tenant)->resultset('Statistics')->search(undef, {
        select => [ \"extract(epoch FROM (now() - max(day)::timestamp))" ],
        as     => ['age'],
      })->first->get_column('age');
    };
    next unless defined $age;
    $output .= _sample('netdisco_stats_age_seconds', floor($age), tenant => $tenant);
  }
  $output .= "\n";

  # -- Backend / worker health -----------------------------------------------
  $output .= _header('netdisco_backends', 'Number of active backend instances');
  $output .= _header('netdisco_workers',  'Total number of worker slots across all backends');

  my $backends_output = '';
  my $workers_output  = '';
  foreach my $tenant (@tenants) {
    my @backends = try {
      schema($tenant)->resultset('DeviceSkip')
        ->search({ device => '255.255.255.255' })->hri->all;
    } catch { () };
    my $tot_workers = 0;
    $tot_workers += $_->{deferrals} for @backends;
    $backends_output .= _sample('netdisco_backends', scalar @backends, tenant => $tenant);
    $workers_output  .= _sample('netdisco_workers',  $tot_workers,     tenant => $tenant);
  }
  $output .= $backends_output . "\n";
  $output .= $workers_output  . "\n";

  # -- Job queue metrics (live, per tenant) ----------------------------------

  # Counts by status
  $output .= _header('netdisco_jobs', 'Number of jobs in the queue by status');
  foreach my $tenant (@tenants) {
    my $rs = try { schema($tenant)->resultset('Admin') } or next;
    foreach my $st (qw/queued done error/) {
      my $count = try { $rs->search({ status => $st })->count } catch { 0 };
      $output .= _sample('netdisco_jobs', $count, tenant => $tenant, status => $st);
    }
  }
  $output .= "\n";

  # Running and stale jobs
  $output .= _header('netdisco_jobs_running', 'Number of jobs currently running');
  foreach my $tenant (@tenants) {
    my $rs = try { schema($tenant)->resultset('Admin') } or next;
    my $running = try {
      $rs->search({ status => 'queued', backend => { '!=' => undef } })->count;
    } catch { 0 };
    $output .= _sample('netdisco_jobs_running', $running, tenant => $tenant);
  }
  $output .= "\n";

  $output .= _header('netdisco_jobs_stale', 'Number of stale jobs (running longer than jobs_stale_after)');
  foreach my $tenant (@tenants) {
    my $rs = try { schema($tenant)->resultset('Admin') } or next;
    my $stale = try {
      $rs->search({
        status  => 'queued',
        backend => { '!=' => undef },
        started => \[ q/<= (LOCALTIMESTAMP - ?::interval)/, setting('jobs_stale_after') ],
      })->count;
    } catch { 0 };
    $output .= _sample('netdisco_jobs_stale', $stale, tenant => $tenant);
  }
  $output .= "\n";

  # Counts by action+status
  $output .= _header('netdisco_jobs_by_action', 'Number of jobs grouped by action and status');
  foreach my $tenant (@tenants) {
    my $rs = try { schema($tenant)->resultset('Admin') } or next;
    my @by_action = try {
      $rs->search(undef, {
        select   => ['action', 'status', { count => '*', -as => 'cnt' }],
        as       => [qw/action status cnt/],
        group_by => [qw/action status/],
      })->hri->all;
    } catch { () };
    foreach my $row (@by_action) {
      $output .= _sample('netdisco_jobs_by_action', $row->{cnt},
        tenant => $tenant, action => $row->{action}, status => $row->{status});
    }
  }
  $output .= "\n";

  # Average duration of completed jobs by action
  $output .= _header('netdisco_job_duration_seconds', 'Average duration of completed jobs by action in seconds');
  foreach my $tenant (@tenants) {
    my $rs = try { schema($tenant)->resultset('Admin') } or next;
    my @durations = try {
      $rs->search(
        { status => 'done', started => { '!=' => undef }, finished => { '!=' => undef } },
        {
          select   => ['action',
            { avg => \"extract(epoch FROM (finished - started))", -as => 'avg_duration' }],
          as       => [qw/action avg_duration/],
          group_by => ['action'],
        }
      )->hri->all;
    } catch { () };
    foreach my $row (@durations) {
      next unless defined $row->{avg_duration};
      $output .= sprintf(qq(netdisco_job_duration_seconds{tenant="%s",action="%s"} %.3f\n),
        $tenant, $row->{action}, $row->{avg_duration});
    }
  }
  $output .= "\n";

  # -- Device inventory metrics ----------------------------------------------

  $output .= _header('netdisco_devices_by_vendor', 'Number of devices grouped by vendor');
  foreach my $tenant (@tenants) {
    my @rows = try {
      schema($tenant)->resultset('Device')->search(undef, {
        select   => [ 'vendor', { count => '*', -as => 'cnt' } ],
        as       => [qw/vendor cnt/],
        group_by => ['vendor'],
      })->hri->all;
    } catch { () };
    foreach my $row (@rows) {
      my $vendor = $row->{vendor} // 'unknown';
      $output .= _sample('netdisco_devices_by_vendor', $row->{cnt},
        tenant => $tenant, vendor => $vendor);
    }
  }
  $output .= "\n";

  $output .= _header('netdisco_devices_by_os', 'Number of devices grouped by OS version');
  foreach my $tenant (@tenants) {
    my @rows = try {
      schema($tenant)->resultset('Device')->search(undef, {
        select   => [ 'os', { count => '*', -as => 'cnt' } ],
        as       => [qw/os cnt/],
        group_by => ['os'],
      })->hri->all;
    } catch { () };
    foreach my $row (@rows) {
      my $os = $row->{os} // 'unknown';
      $output .= _sample('netdisco_devices_by_os', $row->{cnt},
        tenant => $tenant, os => $os);
    }
  }
  $output .= "\n";

  # -- Stale discovery (devices not polled recently) ------------------------
  foreach my $action (
    [ netdisco_devices_discover_stale => 'last_discover' => 'Number of devices not discovered in the last 24 hours' ],
    [ netdisco_devices_macsuck_stale  => 'last_macsuck'  => 'Number of devices not macsucked in the last 24 hours'  ],
    [ netdisco_devices_arpnip_stale   => 'last_arpnip'   => 'Number of devices not arpniped in the last 24 hours'   ],
  ) {
    my ($metric, $col, $help) = @$action;
    $output .= _header($metric, $help);
    foreach my $tenant (@tenants) {
      my $count = try {
        schema($tenant)->resultset('Device')->search({
          -or => [
            $col => undef,
            $col => { '<' => \q|(now() - interval '24 hours')| },
          ],
        })->count;
      } catch { 0 };
      $output .= _sample($metric, $count, tenant => $tenant);
    }
    $output .= "\n";
  }

  # -- Live MAC and ARP counts -----------------------------------------------
  $output .= _header('netdisco_mac_entries', 'Live count of MAC address entries in the node table');
  foreach my $tenant (@tenants) {
    my $count = try { schema($tenant)->resultset('Node')->count } catch { 0 };
    $output .= _sample('netdisco_mac_entries', $count, tenant => $tenant);
  }
  $output .= "\n";

  $output .= _header('netdisco_arp_entries', 'Live count of ARP entries in the node_ip table');
  foreach my $tenant (@tenants) {
    my $count = try { schema($tenant)->resultset('NodeIp')->count } catch { 0 };
    $output .= _sample('netdisco_arp_entries', $count, tenant => $tenant);
  }
  $output .= "\n";

  # -- Slow devices (top 20 per tenant, bounded cardinality) -----------------
  $output .= _header('netdisco_slow_device_duration_seconds',
    'Duration of last completed job for the 20 slowest devices by action (discover/macsuck/arpnip)');
  foreach my $tenant (@tenants) {
    my @rows = try {
      schema($tenant)->resultset('Virtual::SlowDevices')->search(undef)->hri->all;
    } catch { () };
    foreach my $row (@rows) {
      next unless defined $row->{device} and defined $row->{elapsed};
      # elapsed is a PG interval string like "00:00:45.2" - convert to seconds
      my $secs = 0;
      if ($row->{elapsed} =~ m/(\d+):(\d+):(\d+(?:\.\d+)?)/) {
        $secs = $1 * 3600 + $2 * 60 + $3;
      }
      $output .= sprintf(
        qq(netdisco_slow_device_duration_seconds{tenant="%s",device="%s",action="%s"} %.3f\n),
        $tenant, $dev_label->($tenant, $row->{device}), $row->{action}, $secs);
    }
  }
  $output .= "\n";

  # -- SNMP connect failures (DeviceSkip table) ------------------------------
  $output .= _header('netdisco_snmp_failures_devices',
    'Number of devices with at least one SNMP connect failure');
  foreach my $tenant (@tenants) {
    my $count = try {
      schema($tenant)->resultset('DeviceSkip')->search({
        deferrals => { '>' => 0 },
        device    => { '!=' => '255.255.255.255' },
      })->count;
    } catch { 0 };
    $output .= _sample('netdisco_snmp_failures_devices', $count, tenant => $tenant);
  }
  $output .= "\n";

  $output .= _header('netdisco_snmp_failures_by_device',
    'Number of SNMP connect failures per device and backend (top 50 by failure count)');
  foreach my $tenant (@tenants) {
    my @rows = try {
      schema($tenant)->resultset('DeviceSkip')->search({
        deferrals => { '>' => 0 },
        device    => { '!=' => '255.255.255.255' },
      }, {
        order_by => { -desc => 'deferrals' },
        rows     => 50,
      })->hri->all;
    } catch { () };
    foreach my $row (@rows) {
      $output .= sprintf(
        qq(netdisco_snmp_failures_by_device{tenant="%s",device="%s",backend="%s"} %s\n),
        $tenant, $dev_label->($tenant, $row->{device}), $row->{backend}, $row->{deferrals});
    }
  }
  $output .= "\n";

  # -- Per-device port metrics (optional, metrics_ports_per_device: true) --
  if (setting('metrics_ports_per_device')) {
    my %ports_by_device;
    foreach my $tenant (@tenants) {
      $ports_by_device{$tenant} = [try {
        schema($tenant)->resultset('DevicePort')->search({}, {
          select   => [
            'ip',
            { count => '*',                                                  -as => 'total'      },
            \"SUM(CASE WHEN up = 'up' THEN 1 ELSE 0 END)       AS up",
            \"SUM(CASE WHEN up = 'down' THEN 1 ELSE 0 END)     AS down",
            \"SUM(CASE WHEN up_admin = 'down' THEN 1 ELSE 0 END) AS admin_down",
          ],
          as       => [qw/ip total up down admin_down/],
          group_by => ['ip'],
        })->hri->all;
      } catch { () }];
    }

    my @port_metrics = (
      [ netdisco_device_ports_total      => 'total',      'Total number of ports on this device'                   ],
      [ netdisco_device_ports_oper_up    => 'up',         'Number of operationally up ports on this device'        ],
      [ netdisco_device_ports_oper_down  => 'down',       'Number of operationally down ports on this device'      ],
      [ netdisco_device_ports_admin_down => 'admin_down', 'Number of administratively down ports on this device'   ],
    );

    foreach my $m (@port_metrics) {
      my ($metric, $col, $help) = @$m;
      $output .= _header($metric, $help);
      foreach my $tenant (@tenants) {
        for my $row (@{ $ports_by_device{$tenant} }) {
          my $dev = $dev_label->($tenant, $row->{ip});
          $output .= _sample($metric, $row->{$col} // 0, tenant => $tenant, device => $dev);
        }
      }
      $output .= "\n";
    }
  }

  # -- Per-device MAC count (optional, metrics_macs_per_device: true) --------
  if (setting('metrics_macs_per_device')) {
    $output .= _header('netdisco_device_macs',
      'Number of active MAC address entries seen on this device');
    foreach my $tenant (@tenants) {
      my @rows = try {
        schema($tenant)->resultset('Node')->search(
          { active => 'true' },
          {
            select   => ['switch', { count => '*', -as => 'cnt' }],
            as       => [qw/switch cnt/],
            group_by => ['switch'],
          }
        )->hri->all;
      } catch { () };
      foreach my $row (@rows) {
        my $dev = $dev_label->($tenant, $row->{switch});
        $output .= _sample('netdisco_device_macs', $row->{cnt},
          tenant => $tenant, device => $dev);
      }
    }
    $output .= "\n";
  }

  # -- Per-device poll age (optional, metrics_poll_age_per_device: true) -----
  if (setting('metrics_poll_age_per_device')) {
    my %age_by_device;
    foreach my $tenant (@tenants) {
      $age_by_device{$tenant} = [try {
        schema($tenant)->resultset('Device')->search({}, {
          select => [
            'ip', 'dns',
            \"extract(epoch FROM (now() - last_discover)) AS age_discover",
            \"extract(epoch FROM (now() - last_macsuck))  AS age_macsuck",
            \"extract(epoch FROM (now() - last_arpnip))   AS age_arpnip",
          ],
          as => [qw/ip dns age_discover age_macsuck age_arpnip/],
        })->hri->all;
      } catch { () }];
    }

    my @age_metrics = (
      [ netdisco_device_discover_age_seconds => 'age_discover', 'Seconds since last discover completed for this device' ],
      [ netdisco_device_macsuck_age_seconds  => 'age_macsuck',  'Seconds since last macsuck completed for this device'  ],
      [ netdisco_device_arpnip_age_seconds   => 'age_arpnip',   'Seconds since last arpnip completed for this device'   ],
    );

    foreach my $m (@age_metrics) {
      my ($metric, $col, $help) = @$m;
      $output .= _header($metric, $help);
      foreach my $tenant (@tenants) {
        for my $row (@{ $age_by_device{$tenant} }) {
          next unless defined $row->{$col};
          my $dev = ($label_by eq 'dns') ? ($row->{dns} // $row->{ip}) : $row->{ip};
          $output .= _sample($metric, int($row->{$col}), tenant => $tenant, device => $dev);
        }
      }
      $output .= "\n";
    }
  }

  # -- Per-device last job duration + timestamp (optional, metrics_job_duration_per_device: true)
  if (setting('metrics_job_duration_per_device')) {
    my $actions = setting('metrics_job_duration_actions') // [qw/discover macsuck arpnip/];
    my %rows_by_tenant;
    foreach my $tenant (@tenants) {
      $rows_by_tenant{$tenant} = [try {
        schema($tenant)->resultset('Virtual::DeviceJobDurations')->search({
          rn     => 1,
          action => { -in => $actions },
        })->hri->all;
      } catch { () }];
    }

    $output .= _header('netdisco_device_last_job_duration_seconds',
      'Duration in seconds of the most recent completed job per device and action');
    foreach my $tenant (@tenants) {
      foreach my $row (@{ $rows_by_tenant{$tenant} }) {
        next unless defined $row->{device} and defined $row->{duration};
        my $dev = $dev_label->($tenant, $row->{device});
        $output .= sprintf(
          qq(netdisco_device_last_job_duration_seconds{tenant="%s",device="%s",action="%s",status="%s"} %.3f\n),
          $tenant, $dev, $row->{action}, $row->{status}, $row->{duration});
      }
    }
    $output .= "\n";

    $output .= _header('netdisco_device_last_job_timestamp_seconds',
      'Unix timestamp of when the most recent completed job started per device and action');
    foreach my $tenant (@tenants) {
      foreach my $row (@{ $rows_by_tenant{$tenant} }) {
        next unless defined $row->{device} and defined $row->{started_epoch};
        my $dev = $dev_label->($tenant, $row->{device});
        $output .= sprintf(
          qq(netdisco_device_last_job_timestamp_seconds{tenant="%s",device="%s",action="%s",status="%s"} %d\n),
          $tenant, $dev, $row->{action}, $row->{status}, $row->{started_epoch});
      }
    }
    $output .= "\n";
  }

  # -- Per-device neighbor count (optional, metrics_neighbors_per_device: true)
  if (setting('metrics_neighbors_per_device')) {
    $output .= _header('netdisco_device_neighbors',
      'Number of L2 neighbors seen on this device');
    foreach my $tenant (@tenants) {
      my @rows = try {
        schema($tenant)->resultset('DevicePort')->search(
          { remote_ip => { '!=' => undef } },
          {
            select   => ['ip', { count => { distinct => 'remote_ip' }, -as => 'cnt' }],
            as       => [qw/ip cnt/],
            group_by => ['ip'],
          }
        )->hri->all;
      } catch { () };
      foreach my $row (@rows) {
        my $dev = $dev_label->($tenant, $row->{ip});
        $output .= _sample('netdisco_device_neighbors', $row->{cnt},
          tenant => $tenant, device => $dev);
      }
    }
    $output .= "\n";
  }

  # -- Per-device VLAN count (optional, metrics_vlans_per_device: true) ------
  if (setting('metrics_vlans_per_device')) {
    $output .= _header('netdisco_device_vlans',
      'Number of VLANs configured on this device');
    foreach my $tenant (@tenants) {
      my @rows = try {
        schema($tenant)->resultset('DeviceVlan')->search({}, {
          select   => ['ip', { count => '*', -as => 'cnt' }],
          as       => [qw/ip cnt/],
          group_by => ['ip'],
        })->hri->all;
      } catch { () };
      foreach my $row (@rows) {
        my $dev = $dev_label->($tenant, $row->{ip});
        $output .= _sample('netdisco_device_vlans', $row->{cnt},
          tenant => $tenant, device => $dev);
      }
    }
    $output .= "\n";
  }

  # -- PoE global metrics (optional, metrics_poe: true) ---------------------
  if (setting('metrics_poe')) {

    # Query totals once per tenant
    my %poe_totals;
    foreach my $tenant (@tenants) {
      $poe_totals{$tenant} = try {
        schema($tenant)->resultset('Virtual::DevicePoeStatus')->search({}, {
          select => [
            { count => '*',                    -as => 'total_modules'  },
            { sum   => 'power',                -as => 'capacity'       },
            { sum   => 'poe_capable_ports',    -as => 'capable'        },
            { sum   => 'poe_powered_ports',    -as => 'powered'        },
            { sum   => 'poe_disabled_ports',   -as => 'disabled'       },
            { sum   => 'poe_errored_ports',    -as => 'errored'        },
            { sum   => 'poe_power_committed',  -as => 'committed'      },
            { sum   => 'poe_power_delivering', -as => 'delivering'     },
          ],
          as => [qw/total_modules capacity capable powered disabled errored committed delivering/],
        })->hri->first;
      } catch { undef };
    }

    my @poe_scalar_metrics = (
      [ netdisco_poe_modules                => 'total_modules', 'Total number of PoE power modules'                             ],
      [ netdisco_poe_power_capacity_watts   => 'capacity',      'Total installed PoE power capacity in watts'                   ],
      [ netdisco_poe_ports_capable          => 'capable',       'Total number of PoE-capable ports'                             ],
      [ netdisco_poe_ports_powered          => 'powered',       'Total number of ports currently delivering power'              ],
      [ netdisco_poe_ports_disabled         => 'disabled',      'Total number of ports with PoE administratively disabled'      ],
      [ netdisco_poe_ports_errored          => 'errored',       'Total number of ports in fault or error state'                  ],
      [ netdisco_poe_power_committed_watts  => 'committed',     'Total committed PoE power in watts (by class negotiation)'     ],
      [ netdisco_poe_power_delivering_watts => 'delivering',    'Total measured PoE power delivering in watts'                  ],
    );

    foreach my $m (@poe_scalar_metrics) {
      my ($metric, $col, $help) = @$m;
      $output .= _header($metric, $help);
      foreach my $tenant (@tenants) {
        my $t = $poe_totals{$tenant};
        $output .= _sample($metric, ($t ? ($t->{$col} // 0) : 0), tenant => $tenant);
      }
      $output .= "\n";
    }

    $output .= _header('netdisco_poe_modules_by_status',
      'Number of PoE power modules grouped by operational status');
    foreach my $tenant (@tenants) {
      my @rows = try {
        schema($tenant)->resultset('Virtual::DevicePoeStatus')->search({}, {
          select   => ['status', { count => '*', -as => 'cnt' }],
          as       => [qw/status cnt/],
          group_by => ['status'],
        })->hri->all;
      } catch { () };
      foreach my $row (@rows) {
        $output .= _sample('netdisco_poe_modules_by_status', $row->{cnt},
          tenant => $tenant, status => ($row->{status} // 'unknown'));
      }
    }
    $output .= "\n";

    $output .= _header('netdisco_poe_ports_by_class',
      'Number of PoE-capable ports grouped by device power class');
    foreach my $tenant (@tenants) {
      my @rows = try {
        schema($tenant)->resultset('DevicePortPower')->search({}, {
          select   => [ \"COALESCE(class, 'unknown') AS class_label", { count => '*', -as => 'cnt' } ],
          as       => [qw/class cnt/],
          group_by => [ \"COALESCE(class, 'unknown')" ],
        })->hri->all;
      } catch { () };
      foreach my $row (@rows) {
        $output .= _sample('netdisco_poe_ports_by_class', $row->{cnt},
          tenant => $tenant, class => $row->{class});
      }
    }
    $output .= "\n";
  }

  # -- PoE per-device metrics (optional, metrics_poe_per_device: true) ------
  if (setting('metrics_poe_per_device')) {

    # Query per-device totals once per tenant
    my %poe_by_device;
    foreach my $tenant (@tenants) {
      $poe_by_device{$tenant} = [try {
        schema($tenant)->resultset('Virtual::DevicePoeStatus')->search({}, {
          select   => [
            'ip', 'dns',
            { sum   => 'power',                -as => 'capacity'   },
            { sum   => 'poe_capable_ports',    -as => 'capable'    },
            { sum   => 'poe_powered_ports',    -as => 'powered'    },
            { sum   => 'poe_disabled_ports',   -as => 'disabled'   },
            { sum   => 'poe_errored_ports',    -as => 'errored'    },
            { sum   => 'poe_power_committed',  -as => 'committed'  },
            { sum   => 'poe_power_delivering', -as => 'delivering' },
          ],
          as       => [qw/ip dns capacity capable powered disabled errored committed delivering/],
          group_by => ['ip', 'dns'],
        })->hri->all;
      } catch { () }];
    }

    my @per_device_metrics = (
      [ netdisco_poe_device_power_capacity_watts   => 'capacity',   'Installed PoE power capacity in watts for this device'                    ],
      [ netdisco_poe_device_ports_capable          => 'capable',    'Number of PoE-capable ports on this device'                               ],
      [ netdisco_poe_device_ports_powered          => 'powered',    'Number of ports currently delivering power on this device'                ],
      [ netdisco_poe_device_ports_disabled         => 'disabled',   'Number of ports with PoE administratively disabled on this device'        ],
      [ netdisco_poe_device_ports_errored          => 'errored',    'Number of ports in fault or error state on this device'                   ],
      [ netdisco_poe_device_power_committed_watts  => 'committed',  'Committed PoE power in watts for this device (by class negotiation)'      ],
      [ netdisco_poe_device_power_delivering_watts => 'delivering', 'Measured PoE power delivering in watts for this device'                   ],
    );

    foreach my $m (@per_device_metrics) {
      my ($metric, $col, $help) = @$m;
      $output .= _header($metric, $help);
      foreach my $tenant (@tenants) {
        for my $row (@{ $poe_by_device{$tenant} }) {
          my $dev = ($label_by eq 'dns') ? ($row->{dns} // $row->{ip}) : $row->{ip};
          $output .= _sample($metric, $row->{$col} // 0, tenant => $tenant, device => $dev);
        }
      }
      $output .= "\n";
    }
  }

  return $output;
};
}

true;
