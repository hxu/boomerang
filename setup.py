from setuptools import setup, find_packages

README = open('README.rst').read()

setup(
    name="Boomerang",
    version='0.1.0',
    packages=find_packages(),
    description='Boomerang is a toolset for dispatching discrete R processing jobs to Amazon AWS'
                'services then automatically returning the results and tearing down the AWS resources',
    url='https://github.com/hxu/boomerang',
    license='MIT',
    author='Han Xu',
    long_description=README,
    scripts=[
        'bin/boom_fetch',
        'bin/boom_put',
        'bin/boom'
    ],
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Education',
        'Intended Audience :: Science/Research',
        'License (MIT)',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Operating System :: OS Independent',
        'Operating System :: POSIX',
        'Topic :: Education',
        'Topic :: Scientific/Engineering',
        'Topic :: System :: Distributed Computing',
    ],
    install_requires=[
        'boto',
        'fabric',
    ]
)