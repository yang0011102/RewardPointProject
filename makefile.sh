#/bin/bash/
python /home/yy/Project/RewardPoint/setup.py build_ext --inplace;
find /home/yy/Project/RewardPoint/src -maxdepth 1 -name '*.c' -type f -exec rm -rf {} \;
find /home/yy/Project/RewardPoint/InterfaceModules -maxdepth 1 -name '*.c' -type f -exec rm -rf {} \;
find /home/yy/Project/RewardPoint/config -maxdepth 1 -name '*.c' -type f -exec rm -rf {} \;
rm -rf /home/yy/Project/RewardPoint/build;
mv -f tool.cpython-37m-x86_64-linux-gnu.so ./tool/tool.cpython-37m-x86_64-linux-gnu.so;
