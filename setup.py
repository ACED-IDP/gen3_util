from setuptools import setup, find_packages
# import os
# print(os.getcwd())

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

with open('README.md', 'r') as f:
    long_description = f.read()


setup(
    name='gen3_tracker',
    version='0.0.7rc9',
    description='A CLI for adding version control to Gen3 data submission projects.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='walsbr',
    author_email='walsbr@ohsu.edu',
    url='https://github.com/ACED-IDP/gen3_util',
    packages=find_packages(exclude=['tests', 'tests.*']),
    install_requires=requirements,
    include_package_data=True,
    package_data={  # Optional
        '': ['*.yaml'],
    },
    extras_require={
        'dtale': ['dtale'],
    },
    entry_points={
        'console_scripts': [
            'g3t=gen3_tracker.cli:cli',
        ],
    },
)
