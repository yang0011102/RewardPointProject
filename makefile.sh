cd /home/yy/Project/RewardPoint;
python setup.py build_ext --inplace;
find /home/yy/Project/RewardPoint/src  -name '*.c' -type f -exec rm -rf {} \;