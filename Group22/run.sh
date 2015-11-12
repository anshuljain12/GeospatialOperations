#!/bin/bash

source env.conf

answer=1
while [ ${answer} -lt 9 ] && [ ${answer} -gt 0 ]
do
	./hdfs_setup.sh
	echo -e "Operation to run \n1. Union Polygon \n2. ConvexHull \n3. Farthest Pair \n4. Closest Pair \n5. Spatial Join Query - Points \n6. Range Query \n7. SPatial Join- Rectangle \n8. All \nanything else to exit"
	read answer
	
	if [ ${answer} -eq 1 ]; then
		$spark_home/bin/spark-submit --class edu.asu.cse512.Union --master ${master} --jars jts.jar union-0.1.jar ${UnionQueryArgs}
	elif [ ${answer} -eq 2 ]; then
		$spark_home/bin/spark-submit --class edu.asu.cse512.convexHull --master ${master} --jars jts.jar convexHull-0.1.jar ${ConvexHullArgs}
	elif [ ${answer} -eq 3 ]; then
		$spark_home/bin/spark-submit --class edu.asu.cse512.FarthestPair --master ${master} --jars jts.jar farthestPair-0.1.jar ${FarthestPairArgs}
	elif [ ${answer} -eq 4 ]; then
		$spark_home/bin/spark-submit --class edu.asu.cse512.ClosestPair --master ${master} --jars jts.jar closestPair-0.1.jar ${ClosestPairArgs}
	elif [ ${answer} -eq 5 ]; then
		$spark_home/bin/spark-submit --class edu.asu.cse512.Join --master ${master} --jars jts.jar joinQuery-0.1.jar ${SpatialJoinPointArgs}
	elif [ ${answer} -eq 6 ]; then
		$spark_home/bin/spark-submit --class edu.asu.cse512.RangeQuery --master ${master} --jars jts.jar rangeQuery-0.1.jar ${RangeQueryArgs}
	elif [ ${answer} -eq 7 ]; then
		$spark_home/bin/spark-submit --class edu.asu.cse512.Join --master ${master} --jars jts.jar joinQuery-0.1.jar ${SpatialJoinRectArgs}
	elif [ ${answer} -eq 8 ]; then
		$spark_home/bin/spark-submit --class edu.asu.cse512.Union --master ${master} --jars jts.jar union-0.1.jar ${UnionQueryArgs}
		$spark_home/bin/spark-submit --class edu.asu.cse512.convexHull --master ${master} --jars jts.jar convexHull-0.1.jar ${ConvexHullArgs}
		$spark_home/bin/spark-submit --class edu.asu.cse512.FarthestPair --master ${master} --jars jts.jar farthestPair-0.1.jar ${FarthestPairArgs}
		$spark_home/bin/spark-submit --class edu.asu.cse512.ClosestPair --master ${master} --jars jts.jar closestPair-0.1.jar ${ClosestPairArgs}
		$spark_home/bin/spark-submit --class edu.asu.cse512.Join --master ${master} --jars jts.jar joinQuery-0.1.jar ${SpatialJoinPointArgs}
		$spark_home/bin/spark-submit --class edu.asu.cse512.RangeQuery --master ${master} --jars jts.jar rangeQuery-0.1.jar ${RangeQueryArgs}
		$spark_home/bin/spark-submit --class edu.asu.cse512.Join --master ${master} --jars jts.jar joinQuery-0.1.jar ${SpatialJoinRectArgs}	
	else
		echo "Exiting..."
	fi
	echo "Operation completed"
done
