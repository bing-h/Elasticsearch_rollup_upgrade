"""
@version: python3.7.3
@author: bing.he
@contact: bing.he@ihandysoft.com
@file: setup.py
@time: 2020-01-04 17:15
"""

import os
import re

from setuptools import find_packages, setup

install_requires = [
    'PyYAML==3.13',
    'paramiko==2.7.2'
]
# pip install -e '.[test]'
test_requires = [
    'asynctest',
    'pytest',
    'pytest-asyncio',
    'pytest-cov',
    'pytest-mock',
]

here = os.path.dirname(os.path.abspath(__file__))
with open(
        os.path.join(here, 'elasticsearch_rollup_upgrade/__init__.py'), 'r', encoding='utf8'
) as f:
    version = re.search(r'__version__ = \'(.*?)\'', f.read()).group(1)

setup(
    name='elasticsearch_rollup_upgrade',
    version=version,
    license='Proprietary',
    packages=find_packages(exclude=['tests.*', 'tests']),
    zip_safe=False,
    install_requires=install_requires,
    platforms='any',
    extras_require={
        'test': test_requires,
    },
    entry_points={
        'console_scripts': [
            'es-rollup-upgrade = elasticsearch_rollup_upgrade.rolling_upgrades_es:main',
        ],
    },
)
