# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl test.pl'

######################### We start with some black magic to print on failure.

# Change 1..1 below to 1..last_test_to_print .
# (It may become useful if the test is moved to ./t subdirectory.)

BEGIN { $| = 1; print "1..5\n"; }
END {print "not ok 1\n" unless $loaded;}
use Schedule::Parallel;
$loaded = 1;
print "ok 1\n";

######################### End of black magic.

$Schedule::Parallel::verbose = 0;

# Insert your test code below (better if it prints "ok 13"
# (correspondingly "not ok 13") depending on the success of chunk 13
# of the test code):

my $pid = $$;

mkdir "./$pid", 0777 or die "./$pid $!";

my @queue =
	map
	{
		my $a = $_;
		sub{ ! open my $fh, "> ./$pid/$a" }
	}(a..z);

sub testify
{
	++$loaded;

	if( my @unqueue = runqueue @queue, @queue )
	{
		print "not ok $loaded\tUnable to run all jobs.\n";
		die;
	}
	elsif( grep { ! -e "./$pid/$_" } (a..z) )
	{
		print "not ok $loaded\tJobs did not all complete.\n";
		die;
	}
	else
	{
		print "ok $loaded\n";
	}

	unlink <./$pid/?>
}

testify 1;
testify 2;
testify @queue / 2;
testify @queue / 1;

rmdir $pid;

0
__END__
