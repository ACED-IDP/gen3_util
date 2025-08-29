from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

with open('README.md', 'r') as f:
    long_description = f.read()

setup(
    name='calypr-dataframer',
    version='0.1.0',
    description='A tool for generating dataframes from FHIR metadata',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Calypr Team',
    author_email='team@calypr.com',
    url='https://github.com/calypr/dataframer',
    packages=find_packages(exclude=['tests', 'tests.*']),
    install_requires=requirements,
    include_package_data=True,
    extras_require={
        'dtale': ['dtale'],
    },
    entry_points={
        'console_scripts': [
            'calypr-dataframer=calypr_dataframer.cli:cli',
        ],
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Healthcare Industry',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Topic :: Scientific/Engineering :: Medical Science Apps.',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    python_requires='>=3.8',
)