#!/bin/bash

source env.conf

#cd /home/user22/Desktop/GeospatialOperations/GeospatialOperations/
mvn package
echo "$spark_home"
answer=1
while [ ${answer} -lt 9 ] && [ ${answer} -gt 0 ]
do
	./hdfs_setup.sh
	echo "Operation to run 1. Union Polygon, 2. ConvexHull, 3. Farthest Pair, 4. Closest Pair, 5. Spatial Join Query - Points, 6. Range Query, 7. SPatial Join- Rectangle, 8. All"
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
		$spark_home/bin/spark-submit --class "DDS.team22.GeospatialOperations.SpatialJoinQuery" --master ${master} ${execJar} ${arginp}/JoinQueryInput3.csv ${arginp}/JoinQueryInput2.csv ${argout}/JoinQueryResult "point"
	elif [ ${answer} -eq 6 ]; then
		$spark_home/bin/spark-submit --class "DDS.team22.GeospatialOperations.RangeQuery" --master ${master} ${execJar} ${arginp}/RangeQueryTestData.csv ${arginp}/RangeQueryRectangle.csv ${argout}/RangeQueryResult
	elif [ ${answer} -eq 7 ]; then
		$spark_home/bin/spark-submit --class "DDS.team22.GeospatialOperations.SpatialJoinQuery" --master ${master} ${execJar} ${arginp}/JoinQueryInput1.csv ${arginp}/JoinQueryInput2.csv ${argout}/JoinQueryResult "rectangle"
	elif [ ${answer} -eq 8 ]; then
		$spark_home/bin/spark-submit --class "DDS.team22.GeospatialOperations.UnionPolygon" --master ${master} ${execJar} ${arginp}/UnionQueryTestData.csv ${argout}/UnionPolygonResult
		$spark_home/bin/spark-submit --class "DDS.team22.GeospatialOperations.ConvHull" --master ${master} ${execJar} ${arginp}/ConvexHullTestData.csv ${argout}/ConvHullResult
		$spark_home/bin/spark-submit --class "DDS.team22.GeospatialOperations.farthestPair" --master ${master} ${execJar} ${arginp}/FarthestPairTestData.csv ${argout}/farthestPairResult
		$spark_home/bin/spark-submit --class "DDS.team22.GeospatialOperations.ClosestPair" --master ${master} ${execJar} ${arginp}/ClosestPairTestData.csv ${argout}/ClosestPair
		$spark_home/bin/spark-submit --class "DDS.team22.GeospatialOperations.SpatialJoinQuery" --master ${master} ${execJar} ${arginp}/JoinQueryInput3.csv ${arginp}/JoinQueryInput2.csv ${argout}/JoinQueryResult "point"
		$spark_home/bin/spark-submit --class "DDS.team22.GeospatialOperations.RangeQuery" --master ${master} ${execJar} ${arginp}/RangeQueryTestData.csv ${arginp}/RangeQueryRectangle.csv ${argout}/RangeQueryResult
		$spark_home/bin/spark-submit --class "DDS.team22.GeospatialOperations.SpatialJoinQuery" --master ${master} ${execJar} ${arginp}/JoinQueryInput1.csv ${arginp}/JoinQueryInput2.csv ${argout}/JoinQueryResult "rectangle"		
	else
		echo "Enter proper option"
		./hdfs_setup.sh
		break
	fi
	echo "Operation completed"
	
done
