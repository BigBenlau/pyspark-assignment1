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
        log = log.split(', ', 4)
        if len(log) != 5:
            return None
        return log

    def loadRDD(filename):
        textFile = sc.textFile("hdfs://master:9000/test/test01/%s" % filename)
        parsedRDD = textFile.map(logParse).filter(lambda x: x is not None)
        return parsedRDD

    rowrdd = loadRDD("torrent-logs2.txt").cache()

    def getINFONumber(rowrdd):
        INFONumber = rowrdd.filter(lambda x: x[0] == "INFO").count()
        return INFONumber

    def getRepoName(x):
        DividedURL = x[4].split('/')
        RepoName = DividedURL[4] + "/" + DividedURL[5].split("?")[0]
        return RepoName

    def getProcessedRepositoriesNumber(rowrdd):
        RepoUrlList = rowrdd.filter(lambda x: x[3] == "api_client")
        response = RepoUrlList.map(lambda x: getRepoName(x)).groupBy(lambda x: x)
        return response.count()
    
    print("1. Ans :%s\n" % getINFONumber(rowrdd))

    print("2. Ans :%s\n" % getProcessedRepositoriesNumber(rowrdd))

    # print(rowrdd.collect())