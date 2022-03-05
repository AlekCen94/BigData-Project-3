from pyspark.sql import SparkSession, SQLContext
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import *




def parsingData(inputPath,inputPath2, spark):
    df3 = spark.read.options(header='True').csv(inputPath)
    df2 = spark.read.options(header='True').csv(inputPath2)
    df = df3.union(df2)
       
    df.drop('Flow ID', ' Protocol',' Source IP',' Source Port',' Destination IP', ' Destination Port')
    cols = []
    cols = df.columns
    
    del cols[-1]
    for coln in cols:
        df = df.withColumn(coln,col(coln).cast("integer"))  
    df = df[" Flow Duration"," Bwd URG Flags"," Fwd Packet Length Mean", " Label"]
    return df

def init():
    spark = SparkSession.builder.appName('AndroidMalwareML').master("spark://spark-master:7077").getOrCreate()

    return spark

sc = init()
data = parsingData("hdfs://namenode:9000/data/hadoopspark/Ransomware/*.csv","hdfs://namenode:9000/data/hadoopspark/Adware/*.csv", sc)
data.printSchema()

# Index labels, adding metadata to the label column.
# Fit on whole dataset to include all labels in index.
labelIndexer = StringIndexer(inputCol=" Label", outputCol="indexedLabel").fit(data)
col = data.columns
del col[-1]
vecta = VectorAssembler(inputCols=col,outputCol="features")
dataTemp = vecta.transform(data)

# Automatically identify categorical features, and index them.
# We specify maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(dataTemp)

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = dataTemp.randomSplit([0.7, 0.3])

# Train a DecisionTree model.
dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")

# Chain indexers and tree in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, dt])

# Train model.  This also runs the indexers.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("prediction", "indexedLabel", "features").show(5)

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g " % (1.0 - accuracy))

treeModel = model.stages[2]
# summary only
print(treeModel)