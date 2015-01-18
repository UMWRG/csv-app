pyinstaller -y --upx-dir=../../upx ExportCSV.spec
xcopy dist\ExportCSV\* ..\CSVPlugin\plugins\ExportCSV\ /Y /E
copy plugin.xml ..\CSVPlugin\plugins\ExportCSV\
copy icon16.png ..\CSVPlugin\plugins\ExportCSV\
copy icon32.png ..\CSVPlugin\plugins\ExportCSV\
PAUSE