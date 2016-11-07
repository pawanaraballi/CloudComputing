import sys
from pyspark import SparkContext,SparkConf
from pyparsing import Regex, re

# Name: Pawan Araballi
# Student ID: 800935601
# Email: paraballi@uncc.edu

#setting the app name
conf = SparkConf().setAppName("pagerank")
sc = SparkContext(conf=conf)

irank = 0.0
linkCount = 0.0

#storing the input and output arguments
inputFile = str(sys.argv[1])
output = str(sys.argv[2])

# If  number of lines is not defined
#outputlines = 2

def updateRank(entry):
        # Here the each entry will be a tuple in the form of key,pagerank,outlinks
		# Storing the all the three values
	key = entry[0]
        rank = (entry[1])[0]
        olinks = (entry[1])[1]
        returnList = [] # An empty list created to return
        returnList.append((key,(0.0,olinks)))
		# Handling the condition where there is no outlinks
        if not olinks:
                return returnList
        rankToDist = rank/len(olinks)
        for olink in olinks:
                returnList.append((olink,(rankToDist,"")))
        return returnList
		
# Initial parsing and outlinks created 		
def createOlinks(lines):
        if not lines:
                return
        else:
                title = re.search('\<title\>(.*?)\<\/title\>',lines).group(1)
		olinks = re.findall('\[\[(.*?)\]\]',lines)
                if not olinks:
                        olinks = []
		irank = 1.0/linkCount
                return (title,(irank,olinks)) # tuple returned which would be used in updateRank in format (title,(rank,outlinks))

# Reduce function		
def reduceData(x,y):
        rank = x[0]+y[0]
	olinks = []
	if y[1]:
                olinks = y[1]
        if x[1]:
                olinks = x[1]
        return (rank,olinks)

#Function to calculate the page rank 		
def calculateRank(x):
	return (x[0],(x[1][0]*.85+.15 ,x[1][1]))

#Reading the textFile
lines = sc.textFile(inputFile)
titles = lines.flatMap(lambda x: re.findall('\<title\>(.*?)\<\/title\>',x))
datas = lines.flatMap(lambda x: re.findall('\[\[(.*?)\]\]',x)).distinct()
# Calculating the number of titles to calculate N
alltitles = titles.union(datas).distinct()
linkCount = alltitles.count()

dataMap = lines.map(lambda x: createOlinks(x))
for i in range(0, 10):
        mappedOp = dataMap.flatMap(lambda x : updateRank(x))
        dataMap = mappedOp.reduceByKey(lambda x,y : reduceData(x,y))
        dataMap = dataMap.map(lambda x: calculateRank(x))

rdd = sc.parallelize(dataMap.coalesce(1).takeOrdered(linkCount, key = lambda x: -x[1][0])).coalesce(1)
finalrank = rdd.map(lambda x: x[0]+str("   ")+str(x[1][0]))
finalrank.saveAsTextFile(output)
