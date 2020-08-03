# utf-8

from distutils.core import setup
from Cython.Build import cythonize

setup(
    name='Anything you want',
    ext_modules=cythonize([
        # "./InterfaceModules/activity.py", "./InterfaceModules/dd.py"
        # , "./InterfaceModules/order.py", "./InterfaceModules/shoppingCart.py", "./InterfaceModules/upload.py",
        "Interface.py", "dispatcher_switch.py",
    ], language_level=3
    ),
)
