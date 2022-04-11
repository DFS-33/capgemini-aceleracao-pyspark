
# Importando arquivos
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


# Expressoes regulares comuns
# Boas práticas (variáveis constantes em maiusculo)
REGEX_ALPHA    = r'[a-zA-Z]+'
REGEX_INTEGER  = r'[0-9]+'
REGEX_FLOAT    = r'[0-9]+\.[0-9]+'
REGEX_ALPHANUM = r'[0-9a-zA-Z]+'
REGEX_EMPTY_STR= r'[\t ]+$'
REGEX_SPECIAL  = r'[!@#$%&*\(\)_]+'
REGEX_NNUMBER  = r'^N[1-9][0-9]{2,3}([ABCDEFGHJKLMNPRSTUVXWYZ]{1,2})'
REGEX_NNUMBER_INVALID = r'(N0.*$)|(.*[IO].*)'
REGEX_TIME_FMT = r'^(([0-1]?[0-9])|(2[0-3]))([0-5][0-9])$'





# Criando Schema

schema_online_retail = StructType([
    StructField("InvoiceNo",  IntegerType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description",  StringType(), True),
    StructField("quantity",  FloatType(), True),
    StructField("InvoiceDate",  StringType(), True),
    StructField("UnitPrice",   FloatType(), True),
    StructField("CustomerID",  StringType(), True),
	StructField("Country",  StringType(), True)
])


## Criando funcoes
#ef pergunta_1_qa(df):
#	df.withcolumn('InvoiceNo_qa', (
#		F.when(F.col('InvoiceNo').startwith('C'),"canceled"))
#		 .otherwise(F.col"InvoiceNo"))
#	
#	return df
#	




##def pergunta_2_qa(df):

def pergunta_1_qa(df):
    df = df.withColumn("qa_InvoiceNo", 
    F.when(F.col("InvoiceNo").rlike("([0-9a-zA-Z]{6})"), "F")
     .when(F.col("InvoiceNo").startswith("C"), "C").otherwise("OK"))
    print(df.groupBy("qa_InvoiceNo").count().distinct().orderBy("qa_InvoiceNo", ascending=False).show())




 
if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_online_retail)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/online-retail/online-retail.csv"))
	print(df.show())
	pergunta_1_qa(df)


	




