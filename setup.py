from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()
long_description = "run with python -c 'import rats'"

setup(name='ratpy',
      version='1.0.3',
      description='An interpreter and visualiser of RATS files',
      author='Steve Ayrton',
      author_email='s.t.ayrton@icloud.com',
      classifiers=[  # Optional
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Other Audience',
        'Topic :: Scientific/Engineering',
        'Programming Language :: Python :: 3.7'],
      license="BSD",
      packages=find_packages(),
      install_requires=['pandas>=1.2.1',
                        'dash>=1.19.0',
                        'plotly_express>=0.4.1',
                        'plotly>=4.14.3',
                        'numpy>=1.19.5',
                        'dash_bootstrap_components>=0.11.1',
                        'beautifulsoup4>=4.9.3',
                        'pyarrow>=3.0.0',
                        'dash-uploader>=0.4.1',
                        'lxml>=4.6.2'],
      python_requires='>=3.7',
      URL='https://github.com/IonGuide/ratpy')
