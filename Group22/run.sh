#!/bin/bash

source env.conf

answer=1
while [ ${answer} -lt 10 ] && [ ${answer} -gt 0 ]
do
	./hdfs_setup.sh
	echo -e "Operation to run \n1. Union Polygon \n2. ConvexHull \n3. Farthest Pair \n4. Closest Pair \n5. Spatial Join Query - Points \n6. Range Query \n7. SPatial Join- Rectangle \n8. Aggregation \n9. All \nanything else to exit"
	read answer
	
	if [ ${answer} -eq 1 ] || [ ${answer} -eq 9 ]; then
		timeUnion1=$(date +"%s%3N")				
		$spark_home/bin/spark-submit --class edu.asu.cse512.Union --master ${master} --jars jts.jar union-0.1.jar ${UnionQueryArgs}
		timeUnion2=$(date +"%s%3N")
		echo 'Time Taken for Union:' $(($timeUnion2 - $timeUnion1)) 'ms'
	fi	
	if [ ${answer} -eq 2 ] || [ ${answer} -eq 9 ]; then
		timeHull1=$(date +"%s%3N")
		$spark_home/bin/spark-submit --class edu.asu.cse512.convexHull --master ${master} --jars jts.jar convexHull-0.1.jar ${ConvexHullArgs}
		timeHull2=$(date +"%s%3N")
		echo 'Time Taken for Convex Hull: ' $(($timeHull2 - $timeHull1)) 'ms'
	fi	
	if [ ${answer} -eq 3 ] || [ ${answer} -eq 9 ]; then
		timeFPair1=$(date +"%s%3N")
		$spark_home/bin/spark-submit --class edu.asu.cse512.FarthestPair --master ${master} --jars jts.jar farthestPair-0.1.jar ${FarthestPairArgs}
		timeFPair2=$(date +"%s%3N")
		echo 'Time Taken for Farthest Pair: ' $(($timeFPair2 - $timeFPair1)) 'ms'
	fi	
	if [ ${answer} -eq 4 ] || [ ${answer} -eq 9 ]; then
		timeCPair1=$(date +"%s%3N")
		$spark_home/bin/spark-submit --class edu.asu.cse512.ClosestPair --master ${master} --jars jts.jar closestPair-0.1.jar ${ClosestPairArgs}
		timeCPair2=$(date +"%s%3N")
		echo 'Time Taken for Closest Pair: ' $(($timeCPair2 - $timeCPair1)) 'ms'
	fi
	if [ ${answer} -eq 5 ] || [ ${answer} -eq 9 ]; then
		timeJoin1=$(date +"%s%3N")
		$spark_home/bin/spark-submit --class edu.asu.cse512.Join --master ${master} --jars jts.jar joinQuery-0.1.jar ${SpatialJoinPointArgs}
		timeJoin2=$(date +"%s%3N")
		echo 'Time Taken Join Points: ' $(($timeJoin2 - $timeJoin1)) 'ms'
	fi	
	if [ ${answer} -eq 6 ] || [ ${answer} -eq 9 ]; then
		timeRange1=$(date +"%s%3N")
		$spark_home/bin/spark-submit --class edu.asu.cse512.RangeQuery --master ${master} --jars jts.jar rangeQuery-0.1.jar ${RangeQueryArgs}
		timeRange2=$(date +"%s%3N")
		echo 'Time Taken for Range Query: ' $(($timeRange2 - $timeRange1)) 'ms'
	fi	
	if [ ${answer} -eq 7 ] || [ ${answer} -eq 9 ]; then
		timeJoin3=$(date +"%s%3N")
		$spark_home/bin/spark-submit --class edu.asu.cse512.Join --master ${master} --jars jts.jar joinQuery-0.1.jar ${SpatialJoinRectArgs}
		timeJoin4=$(date +"%s%3N")
		echo 'Time Taken for Join Rectangle: ' $(($timeJoin4 - $timeJoin3)) 'ms'
	fi
	if [ ${answer} -eq 8 ] || [ ${answer} -eq 9 ]; then
		timeAgg1=$(date +"%s%3N")
		$spark_home/bin/spark-submit --class edu.asu.cse512.Aggregation --master ${master} --jars jts.jar Aggregation-0.1.jar ${AggregationArgs}
		timeAgg2=$(date +"%s%3N")
		echo 'Time Taken for Aggregation: ' $(($timeAggregation2 - $timeAggregation1)) 'ms'
	fi
	if [ ${answer} -eq 9 ]; then
		echo 'Time Taken for Union:' $(($timeUnion2 - $timeUnion1)) 'ms\n'
		echo 'Time Taken for Convex Hull: ' $(($timeHull2 - $timeHull1)) 'ms\n'
		echo 'Time Taken for Farthest Pair: ' $(($timeFPair2 - $timeFPair1)) 'ms\n'
		echo 'Time Taken for Closest Pair: ' $(($timeCPair2 - $timeCPair1)) 'ms\n'
		echo 'Time Taken Join Points: ' $(($timeJoin2 - $timeJoin1)) 'ms\n'
		echo 'Time Taken for Range Query: ' $(($timeRange2 - $timeRange1)) 'ms\n'
		echo 'Time Taken for Join Rectangle: ' $(($timeJoin4 - $timeJoin3)) 'ms\n'
		echo 'Time Taken for Aggregation: ' $(($timeAggregation2 - $timeAggregation1)) 'ms\n'
	fi 
	echo "Operation completed"
done
