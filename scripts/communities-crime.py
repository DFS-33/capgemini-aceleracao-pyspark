# import pacotes
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType,TimestampType


# Expressoes regulares comuns
# Boas práticas (variáveis constantes em maiusculo)
REGEX_ALPHA    = r'[a-zA-Z]+'
REGEX_INTEGER  = r'[0-9]+'
REGEX_FLOAT    = r'[0-9]+\.[0-9]+'
REGEX_EMPTY_STR= r'[\t ]+$'

# Functions aux 

def check_empty_column(col):
    return (F.col(col).isNull() | (F.col(col) =='') | F.col(col).rlike(REGEX_EMPTY_STR))


# Schema

schema_communities_crime = StructType([
StructField('state', IntegerType(), True),
StructField('county', IntegerType(), True),
StructField('community', IntegerType(), True),
StructField('communityname', StringType(), True),
StructField('fold', IntegerType(), True),
StructField('population', FloatType(), True),
StructField('householdsize', FloatType(), True),
StructField('racepctblack', FloatType(),  True),
StructField('racePctWhite', FloatType(), True),
StructField('racePctAsian', FloatType(), True),
StructField('racePctHisp', FloatType(), True),
StructField('agePct12t21', FloatType(), True),
StructField('agePct12t29', FloatType(), True),
StructField('agePct16t24', FloatType(), True),
StructField('agePct65up', FloatType(), True),
StructField('numbUrban', FloatType(), True),
StructField('pctUrban', FloatType(), True),
StructField('medIncome', FloatType(), True),
StructField('pctWWage', FloatType(), True),
StructField('pctWFarmSelf', FloatType(), True),
StructField('pctWInvInc', FloatType(), True),
StructField('pctWSocSec', FloatType(), True),
StructField('pctWPubAsst', FloatType(), True),
StructField('pctWRetire', FloatType(), True),
StructField('medFamInc', FloatType(), True),
StructField('perCapInc', FloatType(), True),
StructField('whitePerCap', FloatType(), True),
StructField('blackPerCap', FloatType(), True),
StructField('indianPerCap', FloatType(), True),
StructField('AsianPerCap', FloatType(), True),
StructField('OtherPerCap', FloatType(), True),
StructField('HispPerCap', FloatType(), True),
StructField('NumUnderPov', FloatType(), True),
StructField('PctPopUnderPov', FloatType(), True),
StructField('PctLess9thGrade', FloatType(), True),
StructField('PctNotHSGrad', FloatType(), True),
StructField('PctBSorMore', FloatType(), True),
StructField('PctUnemployed', FloatType(), True),
StructField('PctEmploy', FloatType(), True),
StructField('PctEmplManu', FloatType(), True),
StructField('PctEmplProfServ', FloatType(), True),
StructField('PctOccupManu', FloatType(), True),
StructField('PctOccupMgmtProf', FloatType(), True),
StructField('MalePctDivorce', FloatType(), True),
StructField('MalePctNevMarr', FloatType(), True),
StructField('FemalePctDiv', FloatType(), True),
StructField('TotalPctDiv', FloatType(), True),
StructField('PersPerFam', FloatType(), True),
StructField('PctFam2Par', FloatType(), True),
StructField('PctKids2Par', FloatType(), True),
StructField('PctYoungKids2Par', FloatType(), True),
StructField('PctTeen2Par', FloatType(), True),
StructField('PctWorkMomYoungKids', FloatType(), True),
StructField('PctWorkMom', FloatType(), True),
StructField('NumIlleg', FloatType(), True),
StructField('PctIlleg', FloatType(), True),
StructField('NumImmig', FloatType(), True),
StructField('PctImmigRecent', FloatType(), True),
StructField('PctImmigRec5', FloatType(), True),
StructField('PctImmigRec8', FloatType(), True),
StructField('PctImmigRec10', FloatType(), True),
StructField('PctRecentImmig', FloatType(), True),
StructField('PctRecImmig5', FloatType(), True),
StructField('PctRecImmig8', FloatType(), True),
StructField('PctRecImmig10', FloatType(), True),
StructField('PctSpeakEnglOnly', FloatType(), True),
StructField('PctNotSpeakEnglWell', FloatType(), True),
StructField('PctLargHouseFam', FloatType(), True),
StructField('PctLargHouseOccup', FloatType(), True),
StructField('PersPerOccupHous', FloatType(), True),
StructField('PersPerOwnOccHous', FloatType(), True),
StructField('PersPerRentOccHous', FloatType(), True),
StructField('PctPersOwnOccup', FloatType(), True),
StructField('PctPersDenseHous', FloatType(), True),
StructField('PctHousLess3BR', FloatType(), True),
StructField('MedNumBR', FloatType(), True),
StructField('HousVacant', FloatType(), True),
StructField('PctHousOccup', FloatType(), True),
StructField('PctHousOwnOcc', FloatType(), True),
StructField('PctVacantBoarded', FloatType(), True),
StructField('PctVacMore6Mos', FloatType(), True),
StructField('MedYrHousBuilt', FloatType(), True),
StructField('PctHousNoPhone', FloatType(), True),
StructField('PctWOFullPlumb', FloatType(), True),
StructField('OwnOccLowQuart', FloatType(), True),
StructField('OwnOccMedVal', FloatType(), True),
StructField('OwnOccHiQuart', FloatType(), True),
StructField('RentLowQ', FloatType(), True),
StructField('RentMedian', FloatType(), True),
StructField('RentHighQ', FloatType(), True),
StructField('MedRent', FloatType(), True),
StructField('MedRentPctHousInc', FloatType(), True),
StructField('MedOwnCostPctInc', FloatType(), True),
StructField('MedOwnCostPctIncNoMtg', FloatType(), True),
StructField('NumInShelters', FloatType(), True),
StructField('NumStreet', FloatType(), True),
StructField('PctForeignBorn', FloatType(), True),
StructField('PctBornSameState', FloatType(), True),
StructField('PctSameHouse85', FloatType(), True),
StructField('PctSameCity85', FloatType(), True),
StructField('PctSameState85', FloatType(), True),
StructField('LemasSwornFT', FloatType(), True),
StructField('LemasSwFTPerPop', FloatType(), True),
StructField('LemasSwFTFieldOps', FloatType(), True),
StructField('LemasSwFTFieldPerPop', FloatType(), True),
StructField('LemasTotalReq', FloatType(), True),
StructField('LemasTotReqPerPop', FloatType(), True),
StructField('PolicReqPerOffic', FloatType(), True),
StructField('PolicPerPop', FloatType(), True),
StructField('RacialMatchCommPol', FloatType(), True),
StructField('PctPolicWhite', FloatType(), True),
StructField('PctPolicBlack', FloatType(), True),
StructField('PctPolicHisp', FloatType(), True),
StructField('PctPolicAsian', FloatType(), True),
StructField('PctPolicMinor', FloatType(), True),
StructField('OfficAssgnDrugUnits', FloatType(), True),
StructField('NumKindsDrugsSeiz', FloatType(), True),
StructField('PolicAveOTWorked', FloatType(), True),
StructField('LandArea', FloatType(), True),
StructField('PopDens', FloatType(), True),
StructField('PctUsePubTrans', FloatType(), True),
StructField('PolicCars', FloatType(), True),
StructField('PolicOperBudg', FloatType(), True),
StructField('LemasPctPolicOnPatr', FloatType(), True),
StructField('LemasGangUnitDeploy', FloatType(), True),
StructField('LemasPctOfficDrugUn', FloatType(), True),
StructField('PolicBudgPerPop', FloatType(), True),
StructField('ViolentCrimesPerPop', FloatType(), True),
])
 

# Qualidade
def community_qa(dataframe):
	names = dataframe.columns
	for c in names:	# Para cada nome de coluna criar uma coluna nova de qualidade. 	
		dataframe = dataframe.withColumn(f'{c}_qa', 
			F.when(check_empty_column(c), "M").otherwise(F.col(c))
				)
	dataframe = dataframe.select(dataframe.columns[128:256]) # Será que da para usar regex?
	return dataframe


# Transformacao 
def community_proc(dataframe):
	dataframe = dataframe.withColumn('PolicOperBudg',
		F.when(check_empty_column('PolicOperBudg'),0)
		 .otherwise(F.col('PolicOperBudg')))


	dataframe = dataframe.withColumn('ViolentCrimesPerPop',
		F.when(check_empty_column('ViolentCrimesPerPop'),0)
		 .otherwise(F.col('ViolentCrimesPerPop')))

	dataframe = dataframe.withColumn('population',
		F.when(check_empty_column('population'),0)
		 .otherwise(F.col('population')))

	dataframe = dataframe.withColumn('state',
		F.when(check_empty_column('state'),0)
		 .otherwise(F.col('state')))

	return dataframe
	
	

# Questões 


def pergunta_1(dataframe):
	(dataframe.where(F.col('PolicOperBudg').isNotNull())
	   .groupBy(F.col('state'),F.col('communityname'))
	   .agg(F.round(F.sum(F.col('PolicOperBudg')),2).alias('budg_cop'))
	   .orderBy(F.col('budg_cop').desc())
	   .show())

def pergunta_2(dataframe):
	(dataframe.where(F.col('ViolentCrimesPerPop').isNotNull())
	   .groupBy(F.col('state'),F.col('communityname'))
	   .agg(F.round(F.sum(F.col('ViolentCrimesPerPop')),2).alias('ViolentCrimesPerPop'))
	   .orderBy(F.col('ViolentCrimesPerPop').desc())
	   .show(10))



def pergunta_3(dataframe):
	(dataframe.where(F.col('population').isNotNull())
	   .groupBy(F.col('state'), F.col('communityname'))
	   .agg(F.round(F.sum(F.col('population')),2).alias('population'))
	   .orderBy(F.col('population').desc())
	   .show())
	

def pergunta_4(dataframe):
	(dataframe.where(F.col('blackPerCap').isNotNull())
	   .groupBy(F.col('state'), F.col('communityname'))
	   .agg(F.round(F.sum(F.col('blackPerCap')),2).alias("BlackPeople"))
	   .orderBy(F.col('BlackPeople').desc())
	   .show())


def pergunta_5 (dataframe):
	(dataframe.where(F.col('pctWWage').isNotNull())
	   .groupBy(F.col('state'), F.col('communityname'))
	   .agg(F.round(F.sum(F.col('pctWWage')),2).alias('max_salary'))
	   .orderBy(F.col('max_salary').desc())
	   .show())



def pergunta_6 (dataframe)
	(dataframe.where(F.col('agePct12t21').isNotNull())
	   .groupBy(F.col('state'),F.col('communityname'))
	   .agg(F.round(F.sum(F.col('agePct12t21')),2).alias('max_young'))
	   .orderBy(F.col('max_young').desc())
	   .show())

def pergunta_7(dataframe):
    dataframe.agg(F.round(F.corr('PolicOperBudg', 'ViolentCrimesPerPop'), 2).alias('correlation')).show()


def pergunta_8(dataframe):
    dataframe.agg(F.round(F.corr('PctPolicWhite', 'PolicOperBudg'), 2).alias('correlation')).show()


def pergunta_9(dataframe):
    print('Pergunta 9')
    dataframe.agg(F.round(F.corr('population', 'PolicOperBudg'), 2).alias('correlation')).show()


def pergunta_10(dataframe):
    print('Pergunta 10')
    dataframe.agg(F.round(F.corr('population', 'ViolentCrimesPerPop'), 2).alias('correlation')).show()

def pergunta_11(dataframe):
    print('Pergunta 11')
    dataframe.agg(F.round(F.corr('medFamInc', 'ViolentCrimesPerPop'), 2).alias('correlation')).show()



if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Communities & Crime]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_communities_crime)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/communities-crime/communities-crime.csv"))
	#print((df.count(), len(df.columns)))
	df_qa = community_qa(df)  # Quality
	df_proc = community_proc(df) # Transformacao
	#pergunta_1(df_proc)
	#pergunta_2(df_proc)
	#pergunta_3(df_proc)
	#pergunta_4(df_proc)
	#pergunta_5(df_proc)
	#pergunta_6(df_proc)
	#pergunta_7(df_proc)
	#pergunta_8(df_proc)
	#pergunta_9(df_proc)
	#pergunta_10(df_proc)
	#pergunta_11(df_proc)
	pergunta_12(df_proc)
	






	




