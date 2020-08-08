cd /home/yy/Project/RewardPoint || exit ;
python setup.py build_ext --inplace;
find /home/yy/Project/RewardPoint/src  -name '*.c' -type f -exec rm -rf {} \;
find /home/yy/Project/RewardPoint/InterfaceModules  -name '*.c' -type f -exec rm -rf {} \;
find /home/yy/Project/RewardPoint/config  -name '*.c' -type f -exec rm -rf {} \;
rm -r /home/yy/Project/RewardPoint/build;