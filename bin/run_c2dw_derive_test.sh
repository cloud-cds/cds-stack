# @Author: Andong Zhan
# @Date:   2018-03-01 23:11:35
# @Last Modified by:   Andong Zhan
# @Last Modified time: 2018-03-08 16:17:12

np=1
for i in `seq 1 4`;
do
  ng=1
  for j in `seq 1 4`;
  do
    echo $np $ng
    export nprocs=$np
    export num_derive_groups=$ng
    for k in `seq 1 $1`;
    do
      nice -20 python -q -X faulthandler ./etl/clarity2dw/planner.py 2>&1 | tee /mnt/c2dw-test/hcgh-1m-d-p$np-g$ng-$k.log
    done
    ng=$(($ng*2))
  done
  np=$(($np*2))
done

