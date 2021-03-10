# -*- mode: python -*-

block_cipher = None


a = Analysis(['C:\\Users\\ts250088\\PycharmProjects\\new_udi\\read_smx_sheet\\front_end.py'],
             pathex=['C:\\Users\\ts250088\\PycharmProjects\\new_udi'],
             binaries=[],
             datas=[('C:\\Users\\ts250088\\Anaconda3\\envs\\conda_p37\\Lib\\site-packages\\dask\\dask.yaml', './dask'), ('C:\\Users\\ts250088\\Anaconda3\\envs\\conda_p37\\Lib\\site-packages\\distributed\\distributed.yaml', './distributed'),('C:\\Users\\ts250088\\PycharmProjects\\new_udi\\read_smx_sheet\\app_Lib\\script_icon.png','.')],
             hiddenimports=[],
             hookspath=[],
             runtime_hooks=[],
             excludes=['PyQt5'],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          [],
          name='generate_smx_scripts',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          runtime_tmpdir=None,
          console=False )
