import sys
from cx_Freeze import setup, Executable

includes = ["encodings.utf_8",
            "encodings.ascii",
            "lxml._elementpath",
            "lxml.etree",
            "ConfigParser",
            "suds",
            "HydraLib",
            "HydraLib.util",
            "numpy",
            ]

setup(name="Hydra CSV plug-in",
      version="0.1",
      description="Hydra plug-in to import and export CSV files",
      executables=[Executable("ImportCSV.py"), Executable("ExportCSV.py")],
      options={"build_exe": {"includes": includes}}
      )
