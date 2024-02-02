from setuptools import find_packages, setup

setup(
    name='generic_exporters',
    packages=find_packages(),
    use_scm_version={
        "root": ".",
        "relative_to": __file__,
        "local_scheme": "no-local-version",
        "version_scheme": "python-simplified-semver",
    },
    description='A lightweight library that provides generic data export helpers so I can deduplicate code across my various projects.',
    author='BobTheBuidler',
    author_email='bobthebuidlerdefi@gmail.com',
    url='https://github.com/BobTheBuidler/generic_exporters',
    license='MIT',
    setup_requires=[
        'setuptools_scm',
    ],
    package_data={
        'generic_exporters': ['py.typed'],
    },
)

