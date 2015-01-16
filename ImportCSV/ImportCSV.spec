# -*- mode: python -*-
a = Analysis(['ImportCSV.py'],
             pathex=['C:\\Users\\steve\\Documents\\svn\\HYDRA\\HydraPlugins\\CSVplugin\\trunk\\ImportCSV'],
             hiddenimports=[],
             hookspath=None,
             runtime_hooks=None,
             excludes=['_tkinter', 'IPython'])
pyz = PYZ(a.pure)
exe = EXE(pyz,
          a.scripts,
          exclude_binaries=True,
          name='ImportCSV.exe',
          debug=False,
          strip=None,
          upx=True,
          console=True )
coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas,
               strip=None,
               upx=True,
               name='ImportCSV')
