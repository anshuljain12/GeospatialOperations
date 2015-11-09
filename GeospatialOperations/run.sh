#!/bin/bash

source env.conf

#cd /home/user22/Desktop/GeospatialOperations/GeospatialOperations/
mvn package
echo "$spark_home"
answer=1
while [ ${answer} -lt 7 ] && [ ${answer} -gt 0 ]
do
	./hdfs_setup.sh
	echo "Operation to run 1. Union Polygon, 2. ConvexHull, 3. Farthest Pair, 4. Closest Pair, 5. Spatial Join Query, 6. Range Query"
	read answer
	
	if [ ${answer} -eq 1 ]; then
		$spark_home/bin/spark-submit --class "DDS.team22.GeospatialOperations.UnionPolygon" --master ${master} ${execJar} ${arginp}/UnionQueryTestData.csv ${argout}/UnionPolygonResult
	elif [ ${answer} -eq 2 ]; then
		$spark_home/bin/spark-submit --class "DDS.team22.GeospatialOperations.ConvHull" --master ${master} ${execJar} ${arginp}/ConvexHullTestData.csv ${argout}/ConvHullResult
	elif [ ${answer} -eq 3 ]; then
		$spark_home/bin/spark-submit --class "DDS.team22.GeospatialOperations.farthestPair" --master ${master} ${execJar} ${arginp}/FarthestPairTestData.csv ${argout}/farthestPairResult
	elif [ ${answer} -eq 4 ]; then
		$spark_home/bin/spark-submit --class "DDS.team22.GeospatialOperations.ClosestPair" --master ${master} ${execJar} ${arginp}/ClosestPairTestData.csv ${argout}/ClosestPair
	elif [ ${answer} -eq 5 ]; then
		$spark_home/bin/spark-submit --class "DDS.team22.GeospatialOperations.SpatialJoinQuery" --master ${master} ${execJar} ${arginp}/JoinQueryInput1.csv ${arginp}/JoinQueryOutput1.csv ${argout}/SpatialJoinQueryResult
	elif [ ${answer} -eq 6 ]; then
		$spark_home/bin/spark-submit --class "DDS.team22.GeospatialOperations.RangeQuery" --master ${master} ${execJar} ${arginp}/RangeQueryTestData.csv ${arginp}/RangeQueryRectangle.csv ${argout}/RangeQueryResult

	else
		echo "Enter proper option"
		break
	fi
	echo "Operation completed"
	./hdfs_setup.sh
done
