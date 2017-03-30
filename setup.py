from setuptools import setup
from packjobs.packjobs import __version__, __doc__

setup(name='packjobs',
      version=__version__,
      description=__doc__,
      url='http://github.com/migueldiascosta/packjobs',
      author='Miguel Dias Costa',
      author_email='migueldiascosta@gmail.com',
      license='MIT',
      packages=['packjobs'],
      scripts=['packjobs/packjobs.py'],
      zip_safe=False,
      )
