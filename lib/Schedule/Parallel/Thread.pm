##############################################################################
# Schedule::Parallel::Thread
#
# Run jobs in parallel -- fork w/ a bit of boilerplate, queue arriaves
# as an array of closures.
##############################################################################

##############################################################################
# housekeeping
##############################################################################

require 5.8.0;

package Schedule::Parallel::Thread;

use strict;

use Carp;

##############################################################################
# package variables
##############################################################################

our $VERSION = $Schedule::Parallel::VERSION;

# set to false, avoids anything but error messages.

*verbose = \$Schedue::Parallel::verbose;

# used to track the job slots.

our $semaphore = 0;

##############################################################################
# real work begins here
##############################################################################

# need to tickle the semaphore as the job is run, so
# wrap the closure in an anon sub 

sub threadify
{
	# the closure to run.

	my $run = shift
		or die "Bogus threadify: missing job to run";

	# since the threads are detached they will have
	# to up the semaphore for themselves.

	my $job = 
	sub
	{
		&$run;

		$semaphore->up;
	};

	# block here to avoid creating the thread
	# until it is runnable.

	$semaphore->down;

	if( my $thread = Threads->new($job) )
	{
		$thread->detach;
	}
	else
	{
		die "Threadaphobia: $!";
	}

}

sub runqueue
{
	my $maxjobs = shift;

	# $maxjobs threads can be started before
	# the queue blocks on $semaphore->down, 
	# after which it keeps inserting new jobs
	# as the threads up the semaphore.

	$semaphore = Thread::Semaphore->new( $maxjobs );

	# dispatch jobs, blocking on the semaphore
	# until there are no more jobs to dispatch.
	# this will only have $maxjobs threads running
	# at any one time.

	threadify $_ for @_;

	# block until all of the jobs have completed, 
	# at which point they will all have upped the
	# semaphore to its original value.

	$Schedule::Parallel::Thread::semaphore->down( $maxjobs );
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
