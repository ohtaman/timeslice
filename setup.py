from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(name='timeslice',
      version=version,
      description="Time Series analysis",
      long_description="""\
Time Series Analysis""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='datamining timeseries prepare',
      author='Mitsuhisa Ohta',
      author_email='ohtamans@gmail.com',
      url='blog.ohaman.net',
      license='Apache 2.0',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          # -*- Extra requirements: -*-
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
