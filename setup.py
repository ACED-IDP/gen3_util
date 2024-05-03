from setuptools import setup

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

with open('README.md', 'r') as f:
    long_description = f.read()

setup(
    name='g3t',
    version='0.0.4rc1',
    description='A CLI for adding version control to Gen3 data submission projects.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='walsbr',
    author_email='walsbr@ohsu.edu',
    url='https://github.com/ACED-IDP/g3t-git',
    packages=['g3t'],
    install_requires=requirements,
    extras_require={
        'dtale': ['dtale'],
    },
    entry_points={
        'console_scripts': [
            'g3t=g3t.cli:cli',
        ],
    },
)
