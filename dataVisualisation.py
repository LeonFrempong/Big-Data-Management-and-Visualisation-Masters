from pyspark.sql import SparkSession
from pyspark.sql.types import *
import matplotlib.pyplot as plt

# reference:[https://www.youtube.com/watch?v=lZvs-YNk4V0&list=PLaddutuCoakoYOF5Zi6CyPiDTyFl3CMhG&index=25]



schema = StructType([
    StructField("InvoiceNo", IntegerType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", DateType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerID", IntegerType(), True),
    StructField("Country", StringType(), True),
])

spark = SparkSession.builder.appName("DFwithPython").getOrCreate()
sc=spark.sparkContext

df = spark.read.csv("datasetFixed.csv", header=True, sep=",",schema=schema)



dfPlot = df.groupBy("Country").sum("Quantity")

y=dfPlot.toPandas()["Country"].values.tolist()
x=dfPlot.toPandas()["sum(Quantity)"].values.tolist()
plt.title("Countries where most transactions was placed at ")
plt.plot(x,y)
plt.show()



dfPlot = df.groupBy("Country").sum("InvoiceNo")
#plt.plot(dfPlot.Country, dfPlot.InvoiceNo)
y=dfPlot.toPandas()["Country"].values.tolist()
x=dfPlot.toPandas()["sum(InvoiceNo)"].values.tolist()
plt.xlabel("Invoices")
plt.ylabel("country")
plt.title("Countries where most transactions was placed at ")
plt.plot(x,y)
plt.show()





dfPlot = df.groupBy("InvoiceNo",).sum("Quantity")
y = dfPlot.toPandas()["InvoiceNo"].values.tolist()
x = dfPlot.toPandas()["sum(Quantity)"].values.tolist()
plt.plot(x,y)
plt.xlabel("Quantity ")
plt.ylabel("InvoiceNo")
plt.title("Correlation between the invoice number and quantity ")
plt.show()





dfPlot = df.groupBy("InvoiceNo",).sum("Quantity")
y = dfPlot.toPandas()["InvoiceNo"].values.tolist()
x = dfPlot.toPandas()["sum(Quantity)"].values.tolist()


#plt.plot(x,y)
plt.xlabel("Quantity ")
plt.ylabel("InvoiceNo")
plt.title("Correlation between the invoice number and quantity ")
plt.fill(x,y)
plt.show()




