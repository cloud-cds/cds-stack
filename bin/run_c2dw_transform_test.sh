# @Author: Andong Zhan
# @Date:   2018-03-01 23:11:35
# @Last Modified by:   Andong Zhan
# @Last Modified time: 2018-03-06 13:17:57
sl=1
export sem_limit=$sl
for i in `seq 1 $2`;
do
  nice -20 python -q -X faulthandler ./etl/clarity2dw/planner.py 2>&1 | tee /mnt/c2dw-test/hcgh-1m-t-s$sl-$1-$i.log
done

sl=2
export sem_limit=$sl
for i in `seq 1 $2`;
do
  nice -20 python -q -X faulthandler ./etl/clarity2dw/planner.py 2>&1 | tee /mnt/c2dw-test/hcgh-1m-t-s$sl-$1-$i.log
done

sl=4
export sem_limit=$sl
for i in `seq 1 $2`;
do
  nice -20 python -q -X faulthandler ./etl/clarity2dw/planner.py 2>&1 | tee /mnt/c2dw-test/hcgh-1m-t-s$sl-$1-$i.log
done

sl=8
export sem_limit=$sl
for i in `seq 1 $2`;
do
  nice -20 python -q -X faulthandler ./etl/clarity2dw/planner.py 2>&1 | tee /mnt/c2dw-test/hcgh-1m-t-s$sl-$1-$i.log
done

sl=16
export sem_limit=$sl
for i in `seq 1 $2`;
do
  nice -20 python -q -X faulthandler ./etl/clarity2dw/planner.py 2>&1 | tee /mnt/c2dw-test/hcgh-1m-t-s$sl-$1-$i.log
done

sl=32
export sem_limit=$sl
for i in `seq 1 $2`;
do
  nice -20 python -q -X faulthandler ./etl/clarity2dw/planner.py 2>&1 | tee /mnt/c2dw-test/hcgh-1m-t-s$sl-$1-$i.log
done

sl=64
export sem_limit=$sl
for i in `seq 1 $2`;
do
  nice -20 python -q -X faulthandler ./etl/clarity2dw/planner.py 2>&1 | tee /mnt/c2dw-test/hcgh-1m-t-s$sl-$1-$i.log
done