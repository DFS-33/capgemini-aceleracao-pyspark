from operator import contains
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
REGEX_EMPTY_STR= r'[\t ]+$'

# Funcoes auxiliares 
def check_empty_column(col):
    return (F.col(col).isNull() | (F.col(col) == '')| (F.col(col) == '?') )

# Criando Schema
schema_census_income = StructType([
StructField('age', IntegerType(), True),
StructField('workclass', StringType(), True),
StructField('fnlwgt', StringType(), True),
StructField('education', StringType(), True),
StructField('education-num', IntegerType(), True),
StructField('marital-status',StringType(), True),
StructField('occupation', StringType(), True),
StructField('relashionship',StringType(),  True),
StructField('race', StringType(), True),
StructField('sex', StringType(), True),
StructField('capital-gain',  IntegerType(), True),
StructField('capital-loss',  IntegerType(), True),
StructField('hours-per-week',IntegerType(), True),
StructField('native-country', StringType(), True),
StructField('income', StringType(), True),
])

# Qualidade
def census_qa(dataframe):	
	names = dataframe.columns
	for c in names:	# Para cada nome de coluna criar uma coluna nova de qualidade. 	
		dataframe = dataframe.withColumn(f'{c}_qa', 
			F.when(F.col(c).contains('?'), "M").otherwise(F.col(c))
				)
	dataframe = dataframe.select(dataframe.columns[15:30]) # Será que da para usar regex?
	return dataframe


# Transformacao 

def census_proc(dataframe):
	# coluna workclass
	dataframe = dataframe.withColumn('workclass',
		F.when(F.col('workclass').contains('?'),None)
		 .otherwise(F.col('workclass')))
	# coluna occupation
	dataframe = dataframe.withColumn('occupation',
		F.when(F.col('occupation').contains('?'),None)
		 .otherwise(F.col('occupation')))
    # coluna income
	dataframe = dataframe.withColumn('income',
		F.when(F.col('income').contains('?'),None)
		 .otherwise(F.col('income')))
	
	# coluna race
	dataframe = dataframe.withColumn('race',
		F.when(F.col('race').contains('?'),None)
		 .otherwise(F.col('race')))

	return dataframe

	

# Questoes 


def pergunta_1(dataframe):
    print('Pergunta 1')
    (dataframe.where((F.col('income').contains('>50K')) &
              (~F.col('workclass').isNull()))
       .groupBy('workclass', 'income')
       .count()
       .orderBy(F.col('count').desc())
       .show())




def pergunta_2(dataframe):
	(dataframe.groupBy(F.col('race'))
			  .agg((F.round(F.mean('hours-per-week'), 2).alias('mean')))
			  .orderBy(F.col('mean').desc())
			  .show()
	)



def pergunta_3(dataframe):
	(dataframe.groupBy('sex')
			  .count()
			  .withColumn('percent', F.round((F.col('count') / dataframe.count()),2))
			  .show())
# Contagem do agrupamento da coluna sex, sera dividido pela contagem de linhas do Dataframe

def pergunta_4(dataframe):
	pass




def pergunta_5(dataframe):
	(dataframe.groupBy(F.col('occupation'))
			  .agg((F.round(F.mean('hours-per-week'),2).alias('mean_occupation')))
			  .orderBy(F.col('mean_occupation').desc())
			  .show(1)
			  )



def pergunta_6(dataframe):
	(dataframe.groupBy(F.col('occupation'))
			  .agg((F.round(F.count('education'),2).alias('Count_education_per_occupation')))
			  .orderBy(F.col('Count_education_per_occupation').desc())
			  .show(1)
			  )

 

 
def pergunta_7(dataframe):
	(dataframe.groupBy(F.col('occupation'),F.col('sex'))
		      .agg((F.round(F.count('sex'),2).alias('commom_sex')))
			  .orderBy(F.col('commom_sex').desc())
			  .dropDuplicates(['sex'])
			  .show()
	)


def pergunta_8(dataframe):
	(dataframe.where(F.col('education').contains('Doctorate'))
			  .groupBy(F.col('race'),F.col('education'))
		      .agg((F.round(F.count('race'),2).alias('count_per_race')))
			  .orderBy(F.col('count_per_race').desc())
			  .dropDuplicates(['race'])
			  .show()
	)




def pergunta_9(dataframe):
	pass



def pergunta_10(dataframe):
	pass




def pergunta_11(dataframe):
	pass



def pergunta_12(dataframe):
	pass

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Census Income]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_census_income)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/census-income/census-income.csv"))

	df_quality = census_qa(df)
	df_proc = census_proc(df)
	#print(f'{df.count()},{len(df.columns)}')
	#print(f'{df_quality.count()},{len(df_quality.columns)}')
	#print(df_proc.groupBy("sex").count().distinct().orderBy("sex", ascending=True).show())
	#pergunta_1(df_proc)
	#pergunta_2(df_proc)
	pergunta_3(df_proc)
	#pergunta_5(df_proc)
	#pergunta_6(df_proc)
	#pergunta_7(df_proc)
	#pergunta_8(df_proc)
	#pergunta_9(df_proc)
