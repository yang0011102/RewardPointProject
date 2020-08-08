cd /home/yy/Project/RewardPoint || exit ;
find ./tool -maxdepth 1 -name '*.so' -type f -exec rm -rf {} \;
find . -maxdepth 1 -name '*.so' -type f -exec rm -rf {} \;
python setup.py build_ext --inplace;
find ./src -maxdepth 1 -name '*.c' -type f -exec rm -rf {} \;
find ./InterfaceModules -maxdepth 1 -name '*.c' -type f -exec rm -rf {} \;
find ./config -maxdepth 1 -name '*.c' -type f -exec rm -rf {} \;
rm -rf /home/yy/Project/RewardPoint/build;
