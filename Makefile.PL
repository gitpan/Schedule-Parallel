use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.

WriteMakefile
(
	NAME => "Schedule::Parallel",
    'VERSION_FROM'	=> 'lib/Schedule/Parallel.pm',
	(
		$] >= 5.005
		? (ABSTRACT   => 'Run a queue of closures n-way parallel via fork or threads.',
		AUTHOR     => 'Steven Lembark (slembark@knightsbridge.com)')
		: ()
	),
	'dist' => {COMPRESS=>'gzip',SUFFIX=>'gz'}
);

