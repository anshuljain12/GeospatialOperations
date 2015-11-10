#!/bin/bash

source env.conf

answer=1
while [ ${answer} -lt 9 ] && [ ${answer} -gt 0 ]
do
	./hdfs_setup.sh
	echo "Operation to run 1. Union Polygon, 2. ConvexHull, 3. Farthest Pair, 4. Closest Pair, 5. Spatial Join Query - Points, 6. Range Query, 7. SPatial Join- Rectangle, 8. All"
	read answer
	
	if [ ${answer} -eq 1 ]; then
		$spark_home/bin/spark-submit --class "DDS.team22.UnionPolygon.UnionPolygon" --master ${master} UnionPolygon-0.0.1-SNAPSHOT.jar ${UnionQueryArgs}
	elif [ ${answer} -eq 2 ]; then
		$spark_home/bin/spark-submit --class "DDS.team22.ConvexHull.ConvHull" --master ${master} ConvexHull-0.0.1-SNAPSHOT.jar ${ConvexHullArgs}
	elif [ ${answer} -eq 3 ]; then
		$spark_home/bin/spark-submit --class "DDS.team22.FarthestPair.farthestPair" --master ${master} FarthestPair-0.0.1-SNAPSHOT.jar ${FarthestPairArgs}
	elif [ ${answer} -eq 4 ]; then
		$spark_home/bin/spark-submit --class "DDS.team22.ClosestPair.ClosestPair" --master ${master} ClosestPair-0.0.1-SNAPSHOT.jar ${ClosestPairArgs}
	elif [ ${answer} -eq 5 ]; then
		$spark_home/bin/spark-submit --class "DDS.team22.SpatialJoin.SpatialJoinQuery" --master ${master} SpatialJoin-0.0.1-SNAPSHOT.jar ${SpatialJoinPointArgs}
	elif [ ${answer} -eq 6 ]; then
		$spark_home/bin/spark-submit --class "DDS.team22.RangeQuery.RangeQuery" --master ${master} RangeQuery-0.0.1-SNAPSHOT.jar ${RangeQueryArgs}
	elif [ ${answer} -eq 7 ]; then
		$spark_home/bin/spark-submit --class "DDS.team22.SpatialJoin.SpatialJoinQuery" --master ${master} SpatialJoin-0.0.1-SNAPSHOT.jar ${SpatialJoinRectArgs}
	elif [ ${answer} -eq 8 ]; then
		$spark_home/bin/spark-submit --class "DDS.team22.UnionPolygon.UnionPolygon" --master ${master} UnionPolygon-0.0.1-SNAPSHOT.jar ${UnionQueryArgs}
		$spark_home/bin/spark-submit --class "DDS.team22.ConvexHull.ConvHull" --master ${master} ConvexHull-0.0.1-SNAPSHOT.jar ${ConvexHullArgs}
		$spark_home/bin/spark-submit --class "DDS.team22.FarthestPair.farthestPair" --master ${master} FarthestPair-0.0.1-SNAPSHOT.jar ${FarthestPairArgs}
		$spark_home/bin/spark-submit --class "DDS.team22.ClosestPair.ClosestPair" --master ${master} ClosestPair-0.0.1-SNAPSHOT.jar ${ClosestPairArgs}
		$spark_home/bin/spark-submit --class "DDS.team22.SpatialJoin.SpatialJoinQuery" --master ${master} SpatialJoin-0.0.1-SNAPSHOT.jar ${SpatialJoinPointArgs}
		$spark_home/bin/spark-submit --class "DDS.team22.RangeQuery.RangeQuery" --master ${master} RangeQuery-0.0.1-SNAPSHOT.jar ${RangeQueryArgs}
		$spark_home/bin/spark-submit --class "DDS.team22.SpatialJoin.SpatialJoinQuery" --master ${master} SpatialJoin-0.0.1-SNAPSHOT.jar ${SpatialJoinRectArgs}		
	else
		echo "Enter proper option"
		./hdfs_setup.sh
		break
	fi
	echo "Operation completed"
	
done
