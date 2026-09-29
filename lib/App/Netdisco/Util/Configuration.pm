package App::Netdisco::Util::Configuration;

use Dancer qw/:syntax :script/;
use Dancer::Plugin::DBIC 'schema';

use Hash::Merge::Simple;
use MIME::Base64 'decode_base64';
use Scope::Guard 'guard';
use Storable 'dclone';
use Try::Tiny;

use base 'Exporter';
our @EXPORT = ();
our @EXPORT_OK = qw/
  refresh_managed_acl
  load_acls_from_database
  parse_params_to_config
  parse_params_and_config
  apply_config_overrides
/;
our %EXPORT_TAGS = (all => \@EXPORT_OK);

=head1 refresh_acl_from_database( $name )

Given an AccessControlListName result with prefetched C<mappings>, creates
the corresponding C<hosts_groups> ACL in configuration.

=cut

sub refresh_managed_acl {
    my $name = shift;

    foreach my $map (sort {$a->id <=> $b->id} $name->mappings->all) {
        # take every left and optionally right acl (if host_host or host_port) and
        # synthesize them into little host groups
        foreach my $acl ($map->left_acl, $map->right_acl) {
            my $group = 'synthesized_group_'. $acl->id;
            config->{'host_groups'}->{$group} = $acl->rules;
            last if $name->acl_type eq 'host';
        }

        # store in host groups with acl name
        # make a top level group which is list of group: refs (if host)
        if ($name->acl_type eq 'host') {
            push @{ config->{'host_groups'}->{$name->acl_name} },
              ('group:synthesized_group_'. $map->left_acl->id);
        }
        # OR hash of group: to group: refs (if host_host or host_port)
        else {
            config->{'host_groups'}->{$name->acl_name}
              ->{'group:synthesized_group_'. $map->left_acl->id}
              = ('group:synthesized_group_'. $map->right_acl->id);
        }
    }
}

=head1 load_acls_from_database

Loads all the managed ACLs into C<host_groups>.

=cut

sub load_acls_from_database {
    # because this is always called when Netdisco loads, it might happen during tests
    # or other circs when there's no database. exit if so.
    {
        # Temporarily intercept warnings within this block
        local $SIG{__WARN__} = sub {
            my $warning = shift;
            # Silence only the unversioned schema warning
            return if $warning =~ /Your DB is currently unversioned/;
            # Pass all other warnings through
            warn $warning;
        };

        return unless schema(vars->{'tenant'})->get_db_version;
    }

    my @names = schema(vars->{'tenant'})->resultset('AccessControlListName')
      ->search(undef, { prefetch => { mappings => [qw/left_acl right_acl/] } })->all;

    # for each named acl
    refresh_managed_acl($_) for @names;
}

=head1 CONFIGURATION OVERRIDE

Users can override or add to Netdisco configuration from the command line,
or in a job specification in a configured schedule or API-submitted job.
This in turn overrides the NETDISCO_WITH_CONFIGURATION environment variable.

Configuration is provided to a Job in either the C<port> or C<subaction>
(C<extra>) slots. There is a way to provide configuration when these
slots are also used for job parameters.

Configuration can be provided directly as JSON, as simple "k1=v1,k2=v2"
format, or, when job parameters are needed, in a JSON dictionary slot
"C<with>" alongside the job parameter in a dictionary slot "C<value>".

When configuration override is provided to BOTH the C<port> and C<subaction>
(C<extra>) slots the BEHAVIOUR IS UNDEFINED. Best to avoid doing that.

Note that earlier behaviour of providing a bare string as a C<device_auth>
tag hint is now unsupported and C<device_auth_tag_hint> setting can be
used instead.

Also note that this implementation precludes providing a dictionary as
the "extra" configuration to any action (as it will be interpreted and
consumed as configuration setting overrides). Lists and strings remain
supported.

For example:

=over 4

=item * C<undef> (changed to empty string if subaction)

=item * C<unchanged>

=item * C<"unchanged">

=item * C<'"unchanged"'>

=item * C<[{"mac": "string", "port": "string"}]>

=item * C<{"mac": "string", "port": "string"}> (unsupported)

=item * C<{"value": [{"mac": "string", "port": "string"}]}>

=item * C<{"value": {"mac": "string", "port": "string"}}>

=item * C<{"value": '{"mac": "string", "port": "string"}'}>

=item * C<{"value": "unchanged", "with": {"snmptimeout": 5000000}}>

=item * C<{"value": "unchanged", "with": '{"snmptimeout": 5000000}'}>

=item * C<{"value": "unchanged", "with": 'snmptimeout=5000000'}>

=item * C<{"snmptimeout": 5000000}>

=item * C<'{"snmptimeout": 5000000}'>

=item * C<snmptimeout=5000000>

=item * C<{"value": "unchanged", "with": "FAILS"}> (unsupported)

=item * C<{"value": "unchanged", "with": ["FAILS"]}> (unsupported)

=back

=head1 parse_params_to_config

Takes a defined value, works out what has been provided. If there is
configuration to override it applies that. If there is a residual value
to return, it returns that, otherwise returns undef.

=cut

sub parse_params_to_config {
  my ($residual, $overrides) = parse_params_and_config(shift);
  merge_into_configuration($_) for @$overrides;
  return $residual;
}

=head1 parse_params_and_config

As C<parse_params_to_config> but applies nothing. Returns the residual value
and a list reference of the configuration overrides found, in the order they
should be applied.

=cut

# A job is built in the backend manager and run in a poller, which is a
# different process, so the overrides have to travel with the job and be
# applied where it runs. Applying them while parsing changed only the
# manager's configuration, and did so for good.
sub parse_params_and_config {
  my $orig_value = shift;
  my @overrides = ();
  my $residual = _parse_params($orig_value, \@overrides);
  return ($residual, \@overrides);
}

sub _parse_params {
  my ($orig_value, $overrides) = @_;
  return undef unless defined $orig_value;

  # value via "schedule:" deployment.yml would already be a Perl struct
  my $struct = (ref $orig_value ne q{})
    ? $orig_value
    : try { from_json($orig_value) };
    # reminder: from_json of a "" string returns the string, but unquoted throws error
    # so struct could still be a string, or struct reference, or undef on parse error
  my $came_from_json = (((defined $struct)
    and (ref $orig_value eq q{}) and ($struct ne $orig_value)) ? true : false);

  # case when value is a struct but not config (hashref), just leave it alone
  if ((ref $struct ne q{}) and (ref $struct ne ref {})) {
      return $orig_value;
  }

  # case when value is an empty string
  if (($orig_value eq q{}) or (defined $struct and $struct eq q{})) {
      return q{};
  }

  # finally, we have either a lengthy string or a struct
  my $value = ((defined $struct)
    ? $struct
    : $orig_value);

  # if a lengthy string, it could be k=v config
  if (ref $value eq q{}) {
      if ($value =~ m/^(?:(?:[^=,]+)=(?:[^=,]+))(?:,(?:[^=,]+)=(?:[^=,]+))*$/) {
          $value = parse_config_string_to_dict($value);

      }
      else {
          # try to decode base64
          my $decoded = try { from_json(decode_base64($value)) }; # might explode
          if (defined $decoded and ref {} eq ref $decoded) {
              return _parse_params($decoded, $overrides);
          }
          # some other use of subaction (file ref, log comment, etc)
          else {
              return $value;
          }
      }
  }

  # now value is a hashref, look for with/value setup
  my $actual_value = undef;

  if (exists $value->{value}) {
      # if JSON was thawed from the value, refreeze it
      my $inner = delete $value->{value};
      $actual_value = (((ref $inner ne q{}) and $came_from_json)
        ? to_json($inner) : $inner);
  }

  $value = $value->{'with'} if exists $value->{'with'};
  if (ref $value eq ref {}) {
      push @$overrides, $value;
  }
  else {
      # we can recurse to decode a stringified JSON 'with'
      _parse_params($value, $overrides);
  }

  return $actual_value;
}

sub parse_config_string_to_dict {
  my $extra = shift;
  return {} unless
    $extra and (ref $extra eq q{}) and $extra =~ m/=/;

  # must be key1=val1,key2=val2
  my $dict = {};
  my @kvs = split m/,/, $extra;
  foreach my $kv (@kvs) {
      next unless $kv;
      die "bad syntax for subaction, missing =\n" unless $kv =~ m/=/;
      my ($k, $v) = split m/=/, $kv, 2;
      $dict->{$k} = $v;
  }

  return $dict;
}

=head1 apply_config_overrides( \@overrides )

Merges each override into configuration, and returns a guard which puts back
the settings they touched when it goes out of scope.

=cut

sub apply_config_overrides {
  my $overrides = shift || [];
  my $config = config();

  # merge_into_configuration builds a new value for each key it touches and
  # leaves the old one alone, so holding the old reference is enough
  my %saved = map { ($_ => [exists $config->{$_}, $config->{$_}]) }
              map { keys %$_ } @$overrides;

  merge_into_configuration($_) for @$overrides;

  return guard {
    foreach my $key (keys %saved) {
      if ($saved{$key}->[0]) { set($key => $saved{$key}->[1]) }
      else { delete $config->{$key} }
    }
  };
}

sub merge_into_configuration {
    my $newconfig = shift;
    die "bad configuration format\n" unless ref $newconfig eq ref {};
    my $SETTINGS = config();
    $SETTINGS = Hash::Merge::Simple::merge( $SETTINGS, $newconfig );
    set($_ => $SETTINGS->{$_}) for keys %$newconfig;
}

true;