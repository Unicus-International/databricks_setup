from setuptools import setup, find_packages

setup(
    name='dbricks_setup',
    description='Python module for generating a wrapper around the databricks-cli for setting up clusters and secret scopes.',
    license="MIT",
    url="https://github.com/Unicus-International/databricks_setup",
    author='Sindre Osnes',
    author_email='sindreosnes.git@gmail.com',
    version='0.0.1',
    include_package_data=True,
    packages=find_packages( exclude=['tests*']),
    install_requires=[
        'databricks-cli',
    ],
    entry_points={
        'console_scripts': [
            'dbricks_setup=dbricks_setup._cli:cli'
        ]
    },
    test_suite='tests.dbricks_setup_tests',

)
