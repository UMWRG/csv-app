# -*- mode: python -*-
a = Analysis(['ImportCSV.py'],
             pathex=['C:\\Users\\steve\\Documents\\svn\\HYDRA\\HydraPlugins\\CSVplugin'],
             hiddenimports=[],
             hookspath=None,
             runtime_hooks=None)
pyz = PYZ(a.pure)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          name='ImportCSV.exe',
          debug=False,
          strip=None,
          upx=True,
          console=True )
