# @Author: Andong Zhan
# @Date:   2018-03-01 23:11:35
# @Last Modified by:   Andong Zhan
# @Last Modified time: 2018-03-01 23:17:14
for i in `seq 1 $1`;
do
  nice -20 python -q -X faulthandler ./etl/clarity2dw/planner.py 2>&1 | tee /mnt/c2dw-test/hcgh-1m-t-p1-$i.log
done
