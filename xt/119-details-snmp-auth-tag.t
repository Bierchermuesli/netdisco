#!/usr/bin/env perl

use strict;
use warnings;

# The device_auth tag a device last answered to is shown on its Details tab,
# to admins only, so a credential rollover can be followed per device without
# the API or the database. It lives in the community table next to the write
# community, so the route asks for the two tag columns by name.

use Test::More 0.88;
use lib 'xt/lib';
use Test::Netdisco::Snapshot qw/render_template stash_for/;

my $view = 'ajax/device/details.tt';

sub render {
  my (%vars) = @_;
  my $stash = { %{ stash_for($view) }, %vars };
  my ($html, $error) = render_template($view, $stash);
  is $error, undef, 'renders';
  return $html // '';
}

my $admin = sub { $_[0] eq 'admin' ? 1 : 0 };
my $nobody = sub { 0 };

like render(user_has_role => $admin,
            auth_tags => { snmp_auth_tag_read => 'site-a-v3' }),
  qr{SNMP Auth Tag</td>\s*<td>site-a-v3}, 'an admin sees the read tag';

like render(user_has_role => $admin,
            auth_tags => { snmp_auth_tag_read => 'ro', snmp_auth_tag_write => 'rw' }),
  qr{\(write: rw\)}, 'and the write tag when it differs';

unlike render(user_has_role => $admin,
              auth_tags => { snmp_auth_tag_read => 'same', snmp_auth_tag_write => 'same' }),
  qr{\(write:}, 'but not a write tag that is the same as the read one';

unlike render(user_has_role => $nobody,
              auth_tags => { snmp_auth_tag_read => 'site-a-v3' }),
  qr{SNMP Auth Tag}, 'someone who is not an admin sees no tag row';

unlike render(user_has_role => $admin, auth_tags => undef),
  qr{SNMP Auth Tag}, 'and nobody gets an empty row when no tag is stored';

like do { local $/; open my $fh, '<', 'lib/App/Netdisco/Web/Plugin/Device/Details.pm' or die $!; <$fh> },
  qr{columns => \[qw/snmp_auth_tag_read snmp_auth_tag_write/\]},
  'the route reads only the tag columns, not the write community beside them';

done_testing;
