from pyspark import SparkContext,SparkConf

import os
os.environ['SPARK_HOME'] = '/export/server/spark-3.1.2/'
os.environ["PYSPARK_PYTHON"]="/root/anaconda3/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"]="/root/anaconda3/bin/python"
if __name__ == '__main__':
    print('Spark入门案例---WordCount')
    conf = SparkConf().setMaster("local[*]").setAppName("Wordcount")
    sc = SparkContext(conf=conf) 

    rdd_init = sc.textFile("file:///export/data/workspace/class55_parent/_01_pyspark_base/data/word.txt")
    print(rdd_init.collect())
    rdd_flatMap = rdd_init.flatMap(lambda line:line.split(' '))
    print(rdd_flatMap.collect())
    rdd_map = rdd_flatMap.map(lambda word: (word, 1))
    print(rdd_map.collect())

    rdd_res = rdd_map.reduceByKey(lambda a, b: a + b)
    print(rdd_res.collect())

    sc.stop()















