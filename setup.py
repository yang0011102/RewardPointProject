# utf-8

from distutils.core import setup
from Cython.Build import cythonize

setup(
    name='Anything you want',
    ext_modules=cythonize([
        # "./src/tool.pyx",
        "./InterfaceModules/activity.py", "./InterfaceModules/dd.py"
        , "./InterfaceModules/order.py", "./InterfaceModules/shoppingCart.py",
        "./InterfaceModules/upload.py",
        # "./config/config.py","./config/dbconfig.py",
        # "./src/Interface.py",
        # "./src/baseInterface.py",
        # "./src/dispatcher_switch.py",
        # "./src/verify.py",
        # "./src/baseInterface.pyx",
        # "./src/Interface.pyx",
        # "./src/dispatcher_switch.pyx",
        # "./src/verify.pyx",
    ], language_level=3
    ),
)
