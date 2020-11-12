from setuptools import setup

setup(name='dataretrieval',
      version='0.5',
      description='',
      url='',
      author='Timothy Hodson',
      author_email='thodson@usgs.gov',
      license='MIT',
      packages=['dataretrieval', 'dataretrieval.codes'],
      install_requires=[
          'pandas',
          'requests'
      ],
      zip_safe=False)
