
# Importando arquivos
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType,TimestampType


# Expressoes regulares comuns
# Boas práticas (variáveis constantes em maiusculo)
REGEX_INVOICE_N6 = r'^[0-9]{6}$'
REGEX_INVOICE_N5 = r'^[0-9]{5}$'
REGEX_INTEGER  = r'[0-9]+'
REGEX_ALPHA    = r'[a-zA-Z]+'

# Funcoes auxiliares 
def check_empty_column(col):
    return (F.col(col).isNull() | (F.col(col) == ''))


# Criando Schema

schema_online_retail = StructType([
    StructField("InvoiceNo",  IntegerType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description",  StringType(), True),
    StructField("quantity",  IntegerType(), True),
    StructField("InvoiceDate",  StringType(), True),
    StructField("UnitPrice",   StringType(), True),
    StructField("CustomerID",  StringType(), True),
	StructField("Country",  StringType(), True)
])

# função de qualidades

# coluna InvoiceNo
def online_retail_qa(df):

	# coluna InvoiceNo
	df = df.withColumn("qa_InvoiceNo", 
		  F.when(check_empty_column("InvoiceNo"),"M")
	       .when(F.col("InvoiceNo").startswith("C"), "C")
	       .when(F.col("InvoiceNo").rlike("^[0-9]{6}$"), "OK").otherwise("F")
		   )


# coluna StockCode
	df = df.withColumn("qa_StockCode", 
		  F.when(check_empty_column("InvoiceNo"),"M")
		   .when(~F.col("StockCode").rlike("([0-9a-zA-Z]{5})"), "F").otherwise("OK")
		   )

# coluna Description
	df = df.withColumn("qa_description",
		  F.when(check_empty_column('Description'), 'M').otherwise("OK")
		  )


# coluna Quantity
	df = df.withColumn("qa_quantity",
		  F.when(check_empty_column('Quantity'), 'M')
		   .when(~F.col('Quantity').rlike(REGEX_INTEGER), 'F').otherwise("OK")
		   )


# coluna Invoicedate
	df = df.withColumn("qa_invoicedate",
		  F.when(check_empty_column('InvoiceDate'), 'M').otherwise("OK")
		  )

# coluna unit price
	df = df.withColumn("qa_unitprice",
		  F.when(check_empty_column('UnitPrice'),       'M')
		   .when(F.col('UnitPrice') < 0			,       'I')
		   .when(F.col('UnitPrice').rlike(REGEX_ALPHA), 'A').otherwise("OK")
		   )
	

# coluna CustomerID
	df = df.withColumn("qa_customerId", 
	F.when(~F.col("CustomerID").rlike(REGEX_INVOICE_N5), "F").otherwise("OK")
	)


	df = df.withColumn("qa_Country",
		  F.when(check_empty_column('Country'), 'M').otherwise("OK")
		  )
	
	return df


## Função de transformacao

def online_retail_proc(df):
	# coluna  UnitPrice
	df = df.withColumn('UnitPrice', 
		  F.regexp_replace(F.col('UnitPrice'), ',', ".").cast((FloatType())))

	# Coluna InvoiceDate
	df = df.withColumn("InvoiceDate",
		  F.to_timestamp(F.col('InvoiceDate'), 'd/M/yyyy H:m'))


	# Coluna Quantity
	df = df.withColumn('Quantity',
			F.when(F.col('Quantity').isNull(), 0)
			 .when(F.col('Quantity') < 0, 0)
			 .otherwise(F.col('Quantity')))
	return df

# Perguntas

# Pergunta 1 
def pergunta1(df):
	print('Pergunta 1')
	(df.where(F.col('StockCode').rlike('^gift_0001'))
	   .agg(F.round(F.sum(F.expr('Quantity * UnitPrice')), 2).alias('Sum gift cards'))
	   .show())
	print('---------------------------------------------------------------------------')

# pergunta 2 
def pergunta2(df):

		print('Pergunta 2')
		(df.where( F.col('StockCode').startswith('gift_0001'))
	   	   .groupBy( F.month(F.col('InvoiceDate')).alias('month') )
	       .agg( F.round(F.sum(F.expr("Quantity * UnitPrice")), 2).alias('gifts_por_mes') )
	       .orderBy(F.col('month').asc())
	       .show())

# pergunta 3
def pergunta3(df):
		print('Pergunta 3')
		(df.where(F.col('StockCode')=='S')
	   	   .groupBy('StockCode')
	       .agg( F.round(F.sum(F.expr("UnitPrice")), 2).alias('total_sample') )
	       .show())

#pergunta 4 
def pergunta4(df):
	print('Pergunta 4')

	(df.filter(F.col('Quantity') > 0 )
	   .groupBy(F.col('Description'))
	   .agg(F.sum('Quantity').alias('Quantity'))
	   .orderBy(F.col('Quantity').desc())
	   .show(1))

## pergunta 5
def pergunta5(df):
	print('Pergunta 5')

	(df.where((~F.col('StockCode').startswith('C')) &
				   (~F.col('Description').rlike('\?')))
		    .groupBy('Description', F.month('InvoiceDate').alias('month'))
		    .agg(F.sum('Quantity').alias('Quantity'))
		    .orderBy(F.col('Quantity').desc())
		    .dropDuplicates(['month'])
		    .show())

	print('---------------------------------------------------------------------------')


## pergunta 6 
def pergunta6(df):
	(df.where(~F.col('StockCode').contains('C'))
	   .groupBy(F.hour('InvoiceDate').alias('hora_invoice'))
	   .agg(F.round(F.sum(F.expr('UnitPrice * Quantity')), 2).alias('sum_valor_hora'))
	   .orderBy(F.col('sum_valor_hora').desc())
	   .limit(1)
	   .show()
	   )


## pergunta 7 
def pergunta7(df):
	(df.where(~F.col('StockCode').contains('C'))
	   .groupBy(F.month('InvoiceDate').alias('Month'))
	   .agg(F.round(F.sum(F.expr('UnitPrice * Quantity')), 2).alias('sum_valor_mes'))
	   .orderBy(F.col('sum_valor_mes').desc())
	   .limit(1)
	   .show()
	   )



if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_online_retail)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/online-retail/online-retail.csv"))

	df_quality = online_retail_qa(df)   #  dataframe qualidade
	df_proc    = online_retail_proc(df) #  dataframe transformacao

	pergunta1(df_proc)
	pergunta2(df_proc)
	pergunta3(df_proc)
	pergunta4(df_proc)
	pergunta5(df_proc)
	pergunta6(df_proc)
	pergunta7(df_proc)