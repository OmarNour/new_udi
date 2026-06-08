# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path
import sysconfig

block_cipher = None

project_dir = Path().resolve('C:\\Yehia\\UDI\\new_udi').parent
site_packages = Path(sysconfig.get_paths()["purelib"])

a = Analysis(
    [str(project_dir / "new_udi/udi/front_end.py")],
    pathex=[str(project_dir)],
    binaries=[],
    datas=[
        (str(project_dir / "new_udi" / "images/script_icon.png"), "."),
    ],
    hiddenimports=[],
    hookspath=[],
    runtime_hooks=[],
    excludes=["PyQt5"],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name="generate_smx_scripts",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    runtime_tmpdir=None,
    console=False,
)