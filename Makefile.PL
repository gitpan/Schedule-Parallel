use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.

WriteMakefile
(
	NAME => "Schedule::Parallel",
    'VERSION_FROM'	=> 'Parallel.pm',
	(
		$] >= 5.005
		? (ABSTRACT   => 'Run jobs n-way parallel.',
		AUTHOR     => 'Steven Lembark (slembark@knightsbridge.com)')
		: ()
	),
	'dist' => {COMPRESS=>'gzip',SUFFIX=>'gz'}
);

