##############################################################################
# Schedule::Parallel::Fork
#
# Run jobs in parallel -- fork w/ a bit of boilerplate, queue arriaves
# as an array of closures.
#
# See pod for details of execution, the comments are indended for
# maintinence only.
##############################################################################

##############################################################################
# housekeeping
##############################################################################

require 5.005_62;

package Schedule::Parallel::Fork;

use strict;

use Carp;

##############################################################################
# package variables
##############################################################################

our $VERSION = $Schedule::Parallel::VERSION;

##############################################################################
# real work begins here
##############################################################################

# fork with a bit of boilerplate and nastygrams.
# main thing is that the child always performs an exit so that closures 
# that simply return don't cause forkatosis.
#
# parent prints message, error aborts execution, child runs the 
# closure and exits w/ the perl sub's exit code. this means that
# exit codes > 255 are a bad idea on most systems.
#
# Note: doesn't do much good to croak here since we are already
# one level deep into the local module. the caller is using an 
# eval anyway...

sub forkoneover
{
	# the closure to run.

	my $morsel = shift;

	if( (my $forkfull = fork()) > 0 )
	{
		# parent does nothing more
	}
	elsif( defined $forkfull )
	{
		# child passes the exit status of the perl sub call
		# to the caller as our exit status. the O/S will deal
		# with signal values.
		#
		# the truly paranoid could return min( returncode, 255 ),
		# for now it's simpler to trust that most programs will
		# use small, non-zero (or negative) exits for errors.

		exit &$morsel;
	}
	else
	{
		# pass back the fork failure for the caller to deal with.

		die "Phorkafobia: $!";
	}
}

# caller gets back true if we waited for something.
# where there are no jobs left $pid will be zero 
# and the caller gets a false value back.

sub waitfor
{
	# block in wait for a process to exit
	# and deal with its exit status. if there
	# is nothing to wait for this falls through
	# and returns false.

	my $pid = wait;

	if( $pid > 0 )
	{
		# this assumes normal *NIX 16-bit exit values,
		# with a status in the high byte and signum 
		# in the lower. notice that $status is not
		# masked to 8 bits, however. this allows us to
		# deal with non-zero exits on > 16-bit systems.

		if( $? )
		{
			# bad news, boss...

			if( my $status = $? >> 8 )
			{
				die "exit( $status ) by $pid";
			}
			elsif( my $signal = $? & 0xFF )
			{
				die "kill SIG-$signal on $pid";
			}
		}

		# returns true until there is nothing to wait for.

		$pid
	}
	else
	{
		0
	}
}

sub runque
{
	my $maxjobs = shift;

	# dispatch however many jobs we initially want
	# keep running.

	forkoneover $_ for splice @_, 0, $maxjobs;

	# waitfor blocks until a job exits returning,
	# true if there are more jobs outstanding to
	# wait for.
	# when waitfor returns zero the queue has been
	# exhausted.

	while( waitfor )
	{
		# start another job if there is anything left
		# on the queue.

		forkoneover shift @_ if @_;
	}
}

##############################################################################

# keep the use pragma happy

1

__END__

=head1 NAME

Schedule::Parallel::Fork

=head1 SYNOPSIS

	Normally used by Schedule::Parallel to dispatch jobs
	via fork and check for proc's via wait. The main loop
	of runque in forking mode looks like:

	if( waitfor )
	{
		forkoneover shift @_ if @_;
	}


=head1 DESCRIPTION

forkoneover might be vaguely useful for system calls
since it deals with $? gracefully; see comments in 
the code for actual use.

=head1 Copyright

(C) 2001-2002 Steven Lembark, Knightsbridge Solutions

This code is released under the same terms as Perl istelf. Please
see the Perl-5.6.1 distribution (or later) for a full description.

In any case, this code is release as-is, with no implied warranty
of fitness for a particular purpose or warranty of merchantability.

=head1 SEE ALSO

perl(1) perlfork(1)

=cut
