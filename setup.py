from setuptools import setup, find_packages


setup(
      name='mwe_discov_eval',
      version='0.1',
      description='A package for multiword expression discovery',
      url='#',
      author='Nicola Cirillo',
      author_email='nicola.cirillo96@outlook.it',
      license='MIT',
      packages=find_packages(),
      install_requires=[
              'numpy',
              'bounter',
              'nltk',
              'tqdm'
              ],
      zip_safe=False)
