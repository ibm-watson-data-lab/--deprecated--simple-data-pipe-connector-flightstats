from setuptools import setup,find_packages

setup(name='pixiedust_flightpredict',
      version='0.1',
      description='Pixiedust demo of the flight delay predictor tutorial',
      url='https://github.com/ibm-cds-labs/simple-data-pipe-connector-flightstats/tree/master/pixiedust_demo',
      install_requires=['pixiedust'],
      author='David Taieb',
      author_email='david_taieb@us.ibm.com',
      license='Apache 2.0',
      packages=find_packages(),
      zip_safe=False)