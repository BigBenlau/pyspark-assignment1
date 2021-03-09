import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession

if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """

    spark = SparkSession\
        .builder\
        .appName("Assignment1")\
        .getOrCreate()

    sc = spark.sparkContext

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

    rowrdd = loadRDD("torrent-logs.txt").cache()

    def getINFONumber(rowrdd):
        INFONumber = rowrdd.filter(lambda x: x[0] == "INFO").count()
        return INFONumber

    def getRepoName(x):
        dividedURL = x[4].split('/')[4:6]
        if len(dividedURL) == 0:
            return None
        elif len(dividedURL) == 1:
            dividedURL.append('')
        RepoName = (dividedURL[0] + "/" + dividedURL[1]).split("?")[0]
        return RepoName

    def getRepoTotal(rowrdd):
        repoUrlList = rowrdd.filter(lambda x: x[3] == "api_client")
        repoTotal = repoUrlList.map(lambda x: getRepoName(x)).filter(lambda x: x is not None)
        return repoTotal

    def getProcessedRepositoriesNumber(rowrdd):
        repoTotal = getRepoTotal(rowrdd)
        uniqueRepoGroup = repoTotal.groupBy(lambda x: x)
        return uniqueRepoGroup.count()

    def getFailedIDRequest(x):
        if x[4].split(" ")[0] == "Failed":
            return x[2]
        else:
            return None

    def getIDOfMostFailed(rowrdd):
        requestList = rowrdd.filter(lambda x: x[3] == "api_client")
        failedRequestList = requestList.map(lambda x: getFailedIDRequest(x)).filter(lambda x: x is not None).map(lambda x: (x, 1))
        reducedFailedRequestList = failedRequestList.reduceByKey(lambda x, y: x + y)
        mostFailedIDInfo = reducedFailedRequestList.max(key = lambda x: x[1])
        return mostFailedIDInfo

    def getTopFiveActiveRepositories(rowrdd):
        repoTotal = getRepoTotal(rowrdd)
        repoNumList = repoTotal.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
        sortedRepoNumList = repoNumList.sortBy(lambda x: x[1], ascending=False)
        topFiveActiveRepositories = sortedRepoNumList.top(5, key = lambda x: x[1])
        return topFiveActiveRepositories

    print("Q1. Count the number of messages in the category of “INFO”.")
    print("Q1 Ans: The number of “INFO” messages are %d.\n" % getINFONumber(rowrdd))

    print("Q2. Based on the information of retrieval stage “api_client”, count the number of processed repositories.")
    print("Q2 Ans: The number of processed repositories are %d.\n" % getProcessedRepositoriesNumber(rowrdd))

    print("Q3. Which client (downloader id) did most FAILED HTTP requests?")
    mostFailedID, mostFailedIDCount = getIDOfMostFailed(rowrdd)
    print("Q3. Ans: The client id is '%s', it did %d times FAILED HTTP requests.\n" % (mostFailedID, mostFailedIDCount))

    print("Q4. What is the top-5 active repository (based on messages from ghtorrent.rb)?")
    print("Q4. Ans: The top-5 active repository are (format: (repo name, processed number)): ", getTopFiveActiveRepositories(rowrdd))

    