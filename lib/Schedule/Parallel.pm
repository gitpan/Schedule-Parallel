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

our $VERSION = '1.1';

package Schedule::Parallel;

# with only two items Exporter is overkill...

sub import
{

	*{ (caller)[0] . '::runqueue'   } = \&runqueue;

# not yet ready for prime time :-)
#	*{ (caller)[0] . '::usethreads' } = \&usethreads;

}

use strict;

use Carp;
use Config;

##############################################################################
# package variables
##############################################################################

# set to false, avoids anything but error messages.

our $verbose = 0;

##############################################################################
# real work begins here
##############################################################################

{
	# if threading is enabled (check via $Config{useithreads})
	# then set the threading to whatever is passed in; else
	# set it to zero always.

	my $usethreads = 0;

	sub usethreads
	{
		$usethreads = $Config{useithreads} ? $_[0] : 0 if( @_ );

		$usethreads
	}
}

##############################################################################
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
	croak "Bogus maxjobs: $_[0] < 0" unless( $_[0] >= 0 );
	croak "Bogus queue: no jobs" unless @_ > 1;


	# zero job count => run in debug mode.

	if( $_[0] > 0 )
	{

		# catch/throw simpler than having to if-check every
		# one of these.

		eval
		{
			# call S::P::whatever::runque, passing it 
			# the current call's arguments.

			if( usethreads )
			{
				require Schedule::Parallel::Thread;

				&Schedule::Parallel::Thread::runque;
			}
			else
			{
				require Schedule::Parallel::Fork;

				&Schedule::Parallel::Fork::runque;

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

		@_
	}
	else
	{
		# discard the count

		shift;

		# $debug can be changed during debugging 
		# to keep the jobs running continuously.

		my $debug = 1;

		for( @_ ) 
		{
			$DB::single = $debug;

			$_->();
		}

		# always returns an empty list ('clean') in debug mode.

		()
	}
	
}

##############################################################################

# keep the use pragma happy

1

__END__

=head1 NAME

Schedule::Parallel

=head1 SYNOPSIS

  use Schedule::Parallel;

  @unused_portion =  runqueue jobcount, closure, [closure ...];

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

For debugging: if jobcount is zero then the que is run
without forking -- basically via $_->() for @_ on the 
whole que. This saves dealing with fork issues in devlopment. 

=head2 Notes

Running N jobs in parallel makes the assumption that jobs will
consume roughly constant system resource during execution. If
this is not true it may be useful to submit a large que in 
sections with some monitoring to adjust the jobcount parameter
as pieces of the queue are completed. Examples would be raising
the value of jobcount for long-running, mostly-blocked queues
(e.g., web searches) or reducing it to avoid bombarding the 
network if return times are faster.

=head1 AUTHOR

Steven Lembark
Workhorse Computing, LLC

lembark@wrkhors.com

=head1 Copyright

(C) 2001-2004 Workhorse Computing, LLC

This code is released under the same terms as Perl istelf. Please
see the Perl-5.6.1 distribution (or later) for a full description.

In any case, this code is release as-is, with no implied warranty
of fitness for a particular purpose or warranty of merchantability.

=head1 SEE ALSO

perl(1)

=cut
