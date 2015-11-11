#!/bin/bash

source env.conf

answer=1
while [ ${answer} -lt 9 ] && [ ${answer} -gt 0 ]
do
	./hdfs_setup.sh
	echo -e "Operation to run \n1. Union Polygon \n2. ConvexHull \n3. Farthest Pair \n4. Closest Pair \n5. Spatial Join Query - Points \n6. Range Query \n7. SPatial Join- Rectangle \n8. All \nanything else to exit"
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
		echo "Exiting..."
	fi
	echo "Operation completed"
done
