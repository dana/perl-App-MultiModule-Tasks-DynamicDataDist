package App::MultiModule::Tasks::DynamicDataDist;

use 5.006;
use strict;
use warnings FATAL => 'all';
use Message::Transform qw(mtransform);
use Message::Match qw(mmatch);
use IPC::Transit;
use Data::Dumper;

use parent 'App::MultiModule::Task';

=head1 NAME

App::MultiModule::Tasks::DynamicDataDist - Dynamically distribute data sets between groups of MultiModule agents

=cut

=head2 message

=cut

sub message {
    my $self = shift;
    my $message = shift;
#    print STDERR 'MESSAGE: ' . Data::Dumper::Dumper $message;
#    print STDERR "MESSSAGE\n";
    my %args = @_;
    $self->debug('message', message => $message)
        if $self->{debug} > 5;
    my $state = $self->{state};
}

=head2 set_config

=cut
sub set_config {
    my $self = shift;
    my $config = shift;
    $self->{config} = $config;
    $config->{data_groups} = {} unless $config->{data_groups};

    $self->{state} = {} unless $self->{state};
    my $state = $self->{state};
    $state->{agents} = {} unless $state->{agents};
    $state->{data_groups} = {} unless $state->{data_groups};
    if(     $config->{state_seed} and
            ref $config->{state_seed} and
            ref $config->{state_seed} eq 'HASH') {
        mtransform($state, $config->{state_seed});
    }

    $self->named_recur(
        recur_name => 'DynamicDataDist_tick',
        repeat_interval => 1,
        work => sub {
            $self->_tick,
        },
    );

}


sub _tick {
    my $self = shift;
    my $config = $self->{config};
    my $state = $self->{state};

    foreach my $data_group_name (keys %{$config->{data_groups}}) {
        my $data_group_config = $config->{data_groups}->{$data_group_name};
        $state->{data_groups}->{$data_group_name} = {
            slots => {}
        } unless $state->{data_groups}->{$data_group_name};
        my $data_group = $state->{data_groups}->{$data_group_name};

        #Here we need to delete all of the entries in $agent_group that
        #do not match the criteria in $data_group_config->{match}, as applied
        #to all of the agents listed in $state->{agents}
        {   my $new_agents = {};
            foreach my $agent_name (keys %{$state->{agents}}) {
                if(mmatch($state->{agents}->{$agent_name}, $data_group_config->{match})) {
                    $new_agents->{$agent_name} = 1;
                }
            }
            foreach my $agent_name (keys %$data_group) {
                delete $data_group->{$agent_name} unless $new_agents->{$agent_name};
            }
        }

        #Now, $agent_group only contains agents that belong.  Now we find all
        #of those that do not have a current slot
        my $interval = $data_group_config->{interval} || 60;
        my $epoch = time;
        my $current_slot = $epoch % $interval;
        foreach my $agent_name (keys %$data_group) {
            if(not $data_group->{$agent_name}->{slots}->{$current_slot}) {
                #request this data
            }
            #TODO: here we should delete all older slots, except if we have
            #config to keep some number of them.
            #This should also be able to keep some number of slots with
            #DIFFERENT data, no matter how old.  That is, keep the slots
            #that marked the last three changes in data, no matter how old,
            #and delete the rest.  This might be necessary for some of the
            #plugins that require the last N keys
        }
    }
}

sub is_stateful {
    return 'most definitely';
}

=head1 AUTHOR

Dana M. Diederich, C<< <dana@realms.org> >>

=head1 BUGS

Please report any bugs or feature requests through L<https://github.com/dana/perl-App-MultiModule-Tasks-DynamicDataDist/issues>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc App::MultiModule::Tasks::DynamicDataDist


You can also look for information at:

=over 4

=item * Report bugs here:

L<https://github.com/dana/perl-App-MultiModule-Tasks-DynamicDataDist/issues>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/App-MultiModule-Tasks-DynamicDataDist>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/App-MultiModule-Tasks-DynamicDataDist>

=item * Search CPAN

L<https://metacpan.org/module/App::MultiModule::Tasks::DynamicDataDist>

=back

=head1 ACKNOWLEDGEMENTS

=head1 LICENSE AND COPYRIGHT

Copyright 2016 Dana M. Diederich.

This program is free software; you can redistribute it and/or modify it
under the terms of the the Artistic License (2.0). You may obtain a
copy of the full license at:

L<http://www.perlfoundation.org/artistic_license_2_0>

Any use, modification, and distribution of the Standard or Modified
Versions is governed by this Artistic License. By using, modifying or
distributing the Package, you accept this license. Do not use, modify,
or distribute the Package, if you do not accept this license.

If your Modified Version has been derived from a Modified Version made
by someone other than you, you are nevertheless required to ensure that
your Modified Version complies with the requirements of this license.

This license does not grant you the right to use any trademark, service
mark, tradename, or logo of the Copyright Holder.

This license includes the non-exclusive, worldwide, free-of-charge
patent license to make, have made, use, offer to sell, sell, import and
otherwise transfer the Package with respect to any patent claims
licensable by the Copyright Holder that are necessarily infringed by the
Package. If you institute patent litigation (including a cross-claim or
counterclaim) against any party alleging that the Package constitutes
direct or contributory patent infringement, then this Artistic License
to you shall terminate on the date that such litigation is filed.

Disclaimer of Warranty: THE PACKAGE IS PROVIDED BY THE COPYRIGHT HOLDER
AND CONTRIBUTORS "AS IS' AND WITHOUT ANY EXPRESS OR IMPLIED WARRANTIES.
THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
PURPOSE, OR NON-INFRINGEMENT ARE DISCLAIMED TO THE EXTENT PERMITTED BY
YOUR LOCAL LAW. UNLESS REQUIRED BY LAW, NO COPYRIGHT HOLDER OR
CONTRIBUTOR WILL BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, OR
CONSEQUENTIAL DAMAGES ARISING IN ANY WAY OUT OF THE USE OF THE PACKAGE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


=cut

1; # End of App::MultiModule::Tasks::DynamicDataDist
