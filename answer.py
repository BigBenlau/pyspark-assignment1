import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession

if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """

    from pyspark import SparkContext
    sc = SparkContext("local", "First App")

    # spark = SparkSession\
    #     .builder\
    #     .appName("Assignment1")\
    #     .getOrCreate()

    def logParse(log):
        log = log.replace(' -- ', ', ')
        log = log.replace('.rb: ', ', ')
        log = log.replace(', ghtorrent-', ', ')
        return log.split(', ', 4)

    def loadRDD(filename):
        textFile = sc.textFile("hdfs://master:9000/test/test01/%s" % filename)
        parsedRDD = textFile.map(logParse)
        return parsedRDD

    rowrdd = loadRDD("torrent-logs2.txt").cache()

    def getINFONumber(rowrdd):
        response = rowrdd.filter(lambda x: x[0])
        return response

    print("answer!!!!!!!!!!!!!!!!!\n")
    
    print(getINFONumber(rowrdd))

    print(rowrdd.collect())