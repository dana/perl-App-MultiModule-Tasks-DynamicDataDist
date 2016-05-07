#!env perl

use strict;
use warnings FATAL => 'all';
use IPC::Transit;
use File::Slurp;
use File::Temp qw/tempfile tempdir/;
use Data::Dumper;
use POSIX ":sys_wait_h";
use Test::More;
use lib '../lib';

use_ok('App::MultiModule::Tasks::DynamicDataDist');

BEGIN {
    use_ok('App::MultiModule') || die "Failed to load App::MultiModule\n";
    use_ok('App::MultiModule::Test') || die "Failed to load App::MultiModule::Test\n";
    use_ok('App::MultiModule::Test::DynamicDataDist') || die "Failed to load App::MultiModule::Test::DynamicDataDist\n";
}

App::MultiModule::Test::begin();
App::MultiModule::Test::DynamicDataDist::_begin();

my (undef, $errors_log) = tempfile();
my $args = "-q tqueue -p MultiModuleTest:: -o error:$errors_log";
ok my $daemon_pid = App::MultiModule::Test::run_program($args), 'run_program';
END { #just to be damn sure
    kill 9, $daemon_pid;
    unlink $errors_log;
};

my $config = {
    '.multimodule' => {
        config => {
            DynamicDataDist => {
                my_agent_name => 'test_agent',
                _test_transit_translator => {
                    test_agent => {
                        destination => undef,
                        #qname => ....will default to the correct DynamicDataDist
                    },
                    mock_agent => {
                        destination => undef,
                        qname => 'test_out',
                    },
                },
                data_groups => {
                    agent_public_key => {
                        match => {
                            flags => 'f1',
                        },
                        interval => 5,
                    },
                },
                state_seed => {
                    agents => {
                        test_agent => {
                            hostname => 'test_agent',
                            flags => ['f1','f2']
                        },
                        mock_agent => {
                            hostname => 'mock_agent',
                            flags => ['f1'],
                        },
                    },
                },
            },
            MultiModule => {
            },
            Router => {  #router config
                routes => [
                    {   match => {
                            source => 'DynamicDataDist'
                        },
                        forwards => [
                            {   qname => 'test_out' }
                        ],
                    }
                ],
            }
        },
    }
};
ok IPC::Transit::send(qname => 'tqueue', message => $config), 'sent config';

sub message_is {
    my $test_name = shift;
    my $expected = shift;
    my $deletes = shift;
    my $message = eval {
        local $SIG{ALRM} = sub { die "timed out\n"; };
        alarm 22;
        return IPC::Transit::receive(qname => 'test_out');
    };
    alarm 0;
    my $err = $@;
    ok(!$err, "no exception for $test_name");
    if($err) {
        print STDERR "\$get_msg failed: $@\n";
        return undef;
    }
    delete $message->{$_} for @$deletes;
    is_deeply($message, $expected, $test_name);
}

message_is(
    'initial request for data from mock_agent',
    {   dist_get_slots => [
            {   data_group => 'agent_public_key',
                agent_name => 'mock_agent',
                return_destination => 'test_agent',
                return_qname => 'DynamicDataDist',
            }
        ],
    },
    ['.ipc_transit_meta']
);
IPC::Transit::send(qname => 'DynamicDataDist', message => {
    dist_set_slots => [
        {   data_group => 'agent_public_key',
            agent_name => 'mock_agent',
            return_destination => 'test_agent',
            return_qname => 'DynamicDataDist',
            data => {
                something => 'from mock_agent',
            }
        }
    ],
});
#requesting data for both agents
IPC::Transit::send(qname => 'DynamicDataDist', message => {
    dist_get_slots => [
        {   data_group => 'agent_public_key',
            agent_name => 'mock_agent',
            return_destination => 'mock_agent',
            return_qname => 'test_out',
        },
        {   data_group => 'agent_public_key',
            agent_name => 'test_agent',
            return_destination => 'mock_agent',
            return_qname => 'test_out',
        },
    ],
});

message_is(
    'requesting data for both agents',
    {   dist_set_slots => [
            {   data_group => 'agent_public_key',
                agent_name => 'mock_agent',
                return_destination => 'mock_agent',
                return_qname => 'test_out',
                data => {
                    something => 'from mock_agent',
                }
            },
            {   data_group => 'agent_public_key',
                agent_name => 'test_agent',
                return_destination => 'mock_agent',
                return_qname => 'test_out',
                data => {
                    something => 'from test_agent',
                }
            },
        ],
    },
    ['.ipc_transit_meta']
);







##request to exit cleanly
ok IPC::Transit::send(qname => 'tqueue', message => {
    '.multimodule' => {
        control => [
            {   type => 'cleanly_exit',
                exit_externals => 1,
            }
        ],
    }
}), 'sent program exit request';

sleep 6;
ok waitpid($daemon_pid, WNOHANG) == $daemon_pid, 'waitpid';
ok !kill(9, $daemon_pid), 'program exited';

App::MultiModule::Test::finish();
App::MultiModule::Test::DynamicDataDist::_finish();

done_testing();
