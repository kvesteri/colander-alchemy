"""
colander-alchemy
-------------------

Generates colander schemas from SQLAlchemy models.
"""

from setuptools import setup


setup(
    name='colander-alchemy',
    version='0.1.0',
    url='https://github.com/kvesteri/colander-alchemy',
    license='BSD',
    author='Konsta Vesterinen',
    author_email='konsta@fastmonkeys.com',
    description='Generates colander schemas from SQLAlchemy models.',
    long_description=__doc__,
    py_modules=['colander_alchemy'],
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    install_requires=['Flask'],
    test_suite='tests.all_tests',
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
