from setuptools import setup

setup(name='dataretrieval',
      version='0.8',
      description='',
      url='',
      author='Timothy Hodson',
      author_email='thodson@usgs.gov',
      license='CC0',
      packages=['dataretrieval', 'dataretrieval.codes'],
      install_requires=[
          'pandas',
          'requests',
          'typing'
      ],
      zip_safe=False)
