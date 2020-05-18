# Experiments of the paper "An experimental study on design and configuration issues of Spark programs and their impact on performance"

* [AMPLab-Big-Data-Benchmark-Experiments](https://github.com/jbsneto-ppgsc-ufrn/spark-performance-issues-experiments/tree/master/AMPLab-Big-Data-Benchmark-Experiments)
 	* Experiments using the [AMPLab Big Data Benchmark](https://amplab.cs.berkeley.edu/benchmark/) Datasets 
 	* We used the Rankings and UserVisits tables with Scale Factor 5
 	* The URL of the input datasets should be changed in the variables `INPUT\_RANKINGS\_URL` and `INPUT\_USERVISITS\_URL` in the \scripts\run.sh file
 	* The URL of the outputs should also be changed in the variables `OUTPUT_URL` in the \scripts\run.sh file

* [MovieLens-Experiments](https://github.com/jbsneto-ppgsc-ufrn/spark-performance-issues-experiments/tree/master/MovieLens-Experiments)
	* Experiments using the MovieLens Datasets
	* We used the [MovieLens 1M Dataset](http://files.grouplens.org/datasets/movielens/ml-1m.zip) and the [MovieLens Latest Dataset Full](http://files.grouplens.org/datasets/movielens/ml-latest.zip) Datasets
	* The URL of the input datasets should be changed in the variables `INPUT_ML_LATEST_RATINGS_URL`, `INPUT_ML_LATEST_MOVIES_URL`, `INPUT_ML_LATEST_TAGS_URL` and `INPUT_ML_1M_RATINGS_URL` in the \scripts\run.sh file
	* The URL of the outputs should also be changed in the variables `OUTPUT_SIMILARITIES_URL`, `OUTPUT_RECOMMENDATION_URL`, `OUTPUT_GENERAL` and `OUTPUT_URL` in the \scripts\run.sh file

* [Yelp-Experiments](https://github.com/jbsneto-ppgsc-ufrn/spark-performance-issues-experiments/tree/master/Yelp-Experiments)
	* Experiments using the [Yelp](https://www.yelp.com/dataset) Dataset
	* We used only the content of the field `text` in the [review.json](https://www.yelp.com/dataset/documentation/main) file, our dataset contains one review text per row
	* The URL of the input dataset should be changed in the variable `INPUT_URL` in the \scripts\run.sh file
	* The URL of the outputs should also be changed in the variable `OUTPUT_URL` in the \scripts\run.sh file

* [Common](https://github.com/jbsneto-ppgsc-ufrn/spark-performance-issues-experiments/tree/master/Common)
	* Project with the implementation of Custom Spark Listeners used to report performance metrics during the execution of experiments

* [scripts](https://github.com/jbsneto-ppgsc-ufrn/spark-performance-issues-experiments/tree/master/scripts)
	* Folder with configurations and scripts to build and run all experiments
	* The variables in \config.sh should be changed according to the local paths