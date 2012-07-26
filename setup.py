"""
colander-alchemy
-------------------

Generates colander schemas from SQLAlchemy models.
"""

from setuptools import setup, Command
import subprocess


class PyTest(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        errno = subprocess.call(['py.test'])
        raise SystemExit(errno)

setup(
    name='colander-alchemy',
    version='0.1.0',
    url='https://github.com/kvesteri/colander-alchemy',
    license='BSD',
    author='Konsta Vesterinen',
    author_email='konsta@fastmonkeys.com',
    description='Generates colander schemas from SQLAlchemy models.',
    long_description=__doc__,
    packages=['colander_alchemy'],
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    install_requires=[
        'SQLAlchemy>=0.7',
        'pytz>=2011j',
        'colander>=0.9.8'
    ],
    cmdclass={'test': PyTest},
    #test_suite='test_colander_alchemy.suite',
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
