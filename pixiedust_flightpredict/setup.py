from setuptools import setup,find_packages

setup(name='pixiedust_flightpredict',
      version='0.3',
      description='Flight delay predictor application with PixieDust',
      url='https://github.com/ibm-cds-labs/simple-data-pipe-connector-flightstats/tree/master/pixiedust_flightpredict',
      install_requires=['pixiedust'],
      author='David Taieb',
      author_email='david_taieb@us.ibm.com',
      license='Apache 2.0',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False)