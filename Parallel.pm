##############################################################################
# Schedule::Parallel
#
# Run jobs in parallel -- fork w/ a bit of boilerplate, queue arriaves
# as an array of closures.
#
# See pod for details of execution, the comments are indended for
# maintinence only.
#
# This seems more about running a bunch of things in parallel than 
# managing any one of the jobs so I didn't put it under Proc:: or Sys::
#
##############################################################################

##############################################################################
# housekeeping
##############################################################################

require 5.005_62;

package Schedule::Parallel;

# with only one item to export Exporter is overkill...

sub import { *{ (caller)[0] . '::runqueue' } = \&runqueue }

use strict;

use Carp;

##############################################################################
# package variables
##############################################################################

our $VERSION = '1.00';

# set to false, avoids anything but error messages.

our $verbose = 1;

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
		# parent pretty-prints the pid and hands back zero.

		print "\n$$ : forked $forkfull.\n" if $verbose;
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
	
	0
}

# given an array of closures, see that they all get run in 
# parallel with at most N of them running together. 
#
# basically reduces to: fork N of them to begin with and 
# one more every time one of them exits.
#
# arguments are: ( # jobs, array of closures ).
#
# returns unused portion of @_ (queue). this gives 
# false on successful completion of the queue, see
# pod for excruciating details.

sub runqueue
{
	my $maxjobs = shift;

	croak "Bogus maxjobs: $maxjobs <= 0"
		unless( $maxjobs > 0 );

	# whatever's left is the queue of closures.
	#
	# catch/throw simpler than having to if-check every
	# one of these.

	eval
	{
		# immediately start up the maximum number of 
		# jobs to keep running. these will be replaced
		# as they exit.

		forkoneover $_ for splice @_, 0, $maxjobs;

		# block on SIGCHLD from the forks. when nothing
		# is left running we get back -1 from the wait
		# and fall through to the return value.

		while( (my $pid = wait) > 0 )
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
			else
			{
				print "\n$$: Completed: $pid\n" if $verbose;
			}

			# start another job if there is anything left
			# on the queue.

			forkoneover shift @_ if @_;
		}
	};

	warn "$$: Aborted que due to $@" if( $@ );

	print "$$: Remaining queue entries: " . scalar @_ . "\n"
		if $verbose || @_;

	# caller gets back the jobs that failed to run.
	#
	# reason: it is probably simpler for the caller to 
	# re-submit the queue than re-generate it. in a 
	# scalar context this will simply return false if
	# the queue is completed (i.e., shell-like exit).
	#
	# if really huge, non-restartable queues are the 
	# norm it may save some time to return scalar @_,
	# but the perl optimizer will probably do the right
	# thing based on calling context anyway.

	@_
}

##############################################################################

# keep the use pragma happy

1

__END__

=head1 NAME

Schedule::Parallel

=head1 SYNOPSIS

  use Schedule::Parallel;

  @unused_portion =  
	  runqueue( jobcount, closure, [closure ...] );

=head1 DESCRIPTION

Fork with a bit of boilerplate and a maximum number of 
jobs to run at one time (jobcount). The queue (whatever's
left on @_ after shifting off the count) is run in parallel
by forking perl and exiting the sub-process with the 
closure's status.

The caller gets back the unexecuted portion of the queue. In
a scalar context this will return false if the entire execution
succeeded; in an array context it returns the unused portion
for your money bac... er, in order to simplify re-execution where
the calling code can fix the problems (e.g., if the closures
store recovery information).

fork + exit semantics require that code called from
the closures exits zero if it succeeds (i.e., shell-like
returns). Any non-zero exit from a forked job will abort
further processing.

=head2 Notes

Running N jobs in parallel makes the assumption that jobs will
consume roughly constant system resource during execution. If
this is not true it may be useful to submit a large que in 
sections with some monitoring to adjust the jobcount parameter
as pieces of the queue are sumitted. Examples would be raising
the value of jobcount for long-running, mostly-blocked queues
(e.g., web searches) or reducing it to avoid bombarding the 
network if return times are faster.

The leading newlines may look a bit odd but help separate 
status notices from any sub-proc output that they get
buried in. Ditto the leading "$$" on all of the messages.

One minor modification to the arguments improves the logging.
Instead of passing in an array of closures, use an array of
arrays, with the title and a closure. The title is printed,
the closure is executed. In cases where the called subroutine
doesn't give sufficient logging information this might help.
The required change is simply:

	my $item = shift;
	my ( $title, $morsel ) = @$item;

and print $title in the status messages:

	"\n$$: Forked $forkfull running $title\n";

and

	"\n$$: Running $title";


Should provide sufficient logging information for most purposes.


=head1 AUTHOR

Steven Lembark
Knightsbridge Solutions, LLC

slembark@knightsbridge.com

=head1 Copyright

(C) 2001-2002 Steven Lembark, Knightsbridge Solutions

This code is released under the same terms as Perl istelf. Please
see the Perl-5.6.1 distribution (or later) for a full description.

In any case, this code is release as-is, with no implied warranty
of fitness for a particular purpose or warranty of merchantability.

=head1 SEE ALSO

perl(1).

=cut
