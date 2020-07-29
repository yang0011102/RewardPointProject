# utf-8

from distutils.core import setup
from Cython.Build import cythonize

setup(
    name='Anything you want',
    ext_modules=cythonize(["Interface.py", "dispatcher_switch.py"
                            ], language_level=3
        ),
)