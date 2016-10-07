from setuptools import setup

setup(name='trpy',
      version='0.0.1',
      description='trpy bindings for TrailDB',
      author='Jared Flatow',
      author_email='jared.flatow@adroll.com',
      packages=['trpy'],
      scripts=['bin/trpy', 'bin/fnky'],
      cffi_modules=['trpy/__ffic__.py:ffic'],
      setup_requires=['cffi>=1.0.0'],
      install_requires=['cffi>=1.0.0'])
