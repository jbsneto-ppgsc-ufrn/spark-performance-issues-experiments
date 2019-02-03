#!/bin/bash

DIR=`dirname "$0"`
DIR=`cd "${DIR}/.."; pwd`

. "${DIR}/../scripts/config.sh"

CONF_TYPE=$1
CONF=`cluster_config`

JAR=${DIR}/target/scala-2.11/MovieLens-Experiments-assembly-1.0.jar

PACKAGE="br.ufrn.dimap.forall.spark"

INPUT_ML_LATEST_RATINGS_URL="hdfs://master:54310/user/hduser/ml-latest/ratings.csv"
INPUT_ML_LATEST_MOVIES_URL="hdfs://master:54310/user/hduser/ml-latest/movies.csv"
INPUT_ML_LATEST_TAGS_URL="hdfs://master:54310/user/hduser/ml-latest/tags.csv"
INPUT_ML_1M_RATINGS_URL="hdfs://master:54310/user/hduser/ml-1m/ratings.dat"
OUTPUT_SIMILARITIES_URL="hdfs://master:54310/user/hduser/Output/movies-similarities"
OUTPUT_RECOMMENDATION_URL="hdfs://master:54310/user/hduser/Output/movies-recommendation"
OUTPUT_GENERAL="hdfs://master:54310/user/hduser/Output/"

for application in `cat $DIR/scripts/applications.lst`; do
    
    echo "Preparing ${application} ..."
    
    APP="${application}"
    ARGS=""
    
    if [ "${application}" = "MoviesRecommendation_24" ] ; then
        APP="MoviesRecommendation"
        N="24"
        ARGS="$INPUT_ML_1M_RATINGS_URL $OUTPUT_SIMILARITIES_URL $OUTPUT_RECOMMENDATION_URL $N"
    elif [ "${application}" = "MoviesRecommendation_48" ] ; then
        APP="MoviesRecommendation"
        N="48"
        ARGS="$INPUT_ML_1M_RATINGS_URL $OUTPUT_SIMILARITIES_URL $OUTPUT_RECOMMENDATION_URL $N"
    elif [ "${application}" = "MoviesRecommendation_72" ] ; then
        APP="MoviesRecommendation"
        N="72"
        ARGS="$INPUT_ML_1M_RATINGS_URL $OUTPUT_SIMILARITIES_URL $OUTPUT_RECOMMENDATION_URL $N"
    elif [ "${application}" = "MoviesRecommendation_96" ] ; then
        APP="MoviesRecommendation"
        N="96"
        ARGS="$INPUT_ML_1M_RATINGS_URL $OUTPUT_SIMILARITIES_URL $OUTPUT_RECOMMENDATION_URL $N"
    elif [ "${application}" = "MovieLensExplorationNotPersisting" ] ; then
        APP="MovieLensExplorationNotPersisting"
        ARGS="$INPUT_ML_LATEST_RATINGS_URL $INPUT_ML_LATEST_MOVIES_URL $INPUT_ML_LATEST_TAGS_URL $OUTPUT_GENERAL"
    elif [ "${application}" = "MovieLensExplorationPersisting_MEMORY_ONLY" ] ; then
        APP="MovieLensExplorationPersisting"
        PERSISTENCE_LEVEL="MEMORY_ONLY"
        ARGS="$INPUT_ML_LATEST_RATINGS_URL $INPUT_ML_LATEST_MOVIES_URL $INPUT_ML_LATEST_TAGS_URL $OUTPUT_GENERAL $PERSISTENCE_LEVEL"          
    elif [ "${application}" = "MovieLensExplorationPersisting_MEMORY_AND_DISK" ] ; then
        APP="MovieLensExplorationPersisting"
        PERSISTENCE_LEVEL="MEMORY_AND_DISK"
        ARGS="$INPUT_ML_LATEST_RATINGS_URL $INPUT_ML_LATEST_MOVIES_URL $INPUT_ML_LATEST_TAGS_URL $OUTPUT_GENERAL $PERSISTENCE_LEVEL" 
    elif [ "${application}" = "MovieLensExplorationPersisting_MEMORY_ONLY_SER" ] ; then
        APP="MovieLensExplorationPersisting"
        PERSISTENCE_LEVEL="MEMORY_ONLY_SER" 
        ARGS="$INPUT_ML_LATEST_RATINGS_URL $INPUT_ML_LATEST_MOVIES_URL $INPUT_ML_LATEST_TAGS_URL $OUTPUT_GENERAL $PERSISTENCE_LEVEL"
    elif [ "${application}" = "MovieLensExplorationPersisting_DISK_ONLY" ] ; then
        APP="MovieLensExplorationPersisting"
        PERSISTENCE_LEVEL="DISK_ONLY"
        ARGS="$INPUT_ML_LATEST_RATINGS_URL $INPUT_ML_LATEST_MOVIES_URL $INPUT_ML_LATEST_TAGS_URL $OUTPUT_GENERAL $PERSISTENCE_LEVEL"
    elif [ "${application}" = "MoviesRatingsAverageWithBroadcastVariable" ] ; then
        APP="MoviesRatingsAverageWithBroadcastVariable"
        OUTPUT_URL="hdfs://master:54310/user/hduser/Output/movies-ratings-average"
        ARGS="$INPUT_ML_LATEST_RATINGS_URL $OUTPUT_URL"
    elif [ "${application}" = "MoviesRatingsAverageWithoutBroadcastVariable" ] ; then
        APP="MoviesRatingsAverageWithoutBroadcastVariable"
        OUTPUT_URL="hdfs://master:54310/user/hduser/Output/movies-ratings-average"
        ARGS="$INPUT_ML_LATEST_RATINGS_URL $OUTPUT_URL"
    fi
    
        
    CLASS="--class ${PACKAGE}.${APP}"
    
    echo "Running ${application}..."
    
    ${SPARK_SUBMIT} ${CLASS} ${CONF} ${JAR} ${ARGS}
	
	echo "Removing Outputs"
	
	${HDFS_RM} ${HDFS_OUTPUT_FOLDER}
	
	/usr/local/spark/sbin/stop-slaves.sh
    /usr/local/spark/sbin/start-slaves.sh    
    sleep 15s
    
done