
# coding: utf-8

# # Assignment 3
# 
# Welcome to Assignment 3. This will be even more fun. Now we will calculate statistical measures on the test data you have created.
# 
# YOU ARE NOT ALLOWED TO USE ANY OTHER 3RD PARTY LIBRARIES LIKE PANDAS. PLEASE ONLY MODIFY CONTENT INSIDE THE FUNCTION SKELETONS
# Please read why: https://www.coursera.org/learn/exploring-visualizing-iot-data/discussions/weeks/3/threads/skjCbNgeEeapeQ5W6suLkA
# . Just make sure you hit the play button on each cell from top to down. There are seven functions you have to implement. Please also make sure than on each change on a function you hit the play button again on the corresponding cell to make it available to the rest of this notebook.
# Please also make sure to only implement the function bodies and DON'T add any additional code outside functions since this might confuse the autograder.
# 
# So the function below is used to make it easy for you to create a data frame from a cloudant data frame using the so called "DataSource" which is some sort of a plugin which allows ApacheSpark to use different data sources.
# 

# All functions can be implemented using DataFrames, ApacheSparkSQL or RDDs. We are only interested in the result. You are given the reference to the data frame in the "df" parameter and in case you want to use SQL just use the "spark" parameter which is a reference to the global SparkSession object. Finally if you want to use RDDs just use "df.rdd" for obtaining a reference to the underlying RDD object. 
# 
# Let's start with the first function. Please calculate the minimal temperature for the test data set you have created. We've provided a little skeleton for you in case you want to use SQL. You can use this skeleton for all subsequent functions. Everything can be implemented using SQL only if you like.

# In[1]:


def minTemperature(df,spark):
    row = df.agg({'temperature': 'min'}).collect()[0]
    min_temp = row['min(temperature)']
    return min_temp


# Please now do the same for the mean of the temperature

# In[2]:


def meanTemperature(df,spark):
    row = df.agg({'temperature': 'avg'}).collect()[0]
    avg_temp = row['avg(temperature)']
    return avg_temp


# Please now do the same for the maximum of the temperature

# In[3]:


def maxTemperature(df,spark):
    row = df.agg({'temperature': 'max'}).collect()[0]
    max_temp = row['max(temperature)']
    return max_temp


# Please now do the same for the standard deviation of the temperature

# In[22]:


from  math import sqrt

def sdTemperature(df,spark):
    temprddrow = df.select('temperature').rdd #in row(temp=x) format
    temprdd = temprddrow.map(lambda (x) : x["temperature"]) #only numbers
    temp = temprdd.filter(lambda x: x is not None).filter(lambda x: x != "") #remove None params
    n = float(temp.count())
    sum=temp.sum()
    mean =sum/n
    from math import sqrt
    sd=sqrt(temp.map(lambda x : pow(x-mean,2)).sum()/n)
    return sd


# Please now do the same for the skew of the temperature. Since the SQL statement for this is a bit more complicated we've provided a skeleton for you. You have to insert custom code at four position in order to make the function work. Alternatively you can also remove everything and implement if on your own. Note that we are making use of two previously defined functions, so please make sure they are correct. Also note that we are making use of python's string formatting capabilitis where the results of the two function calls to "meanTemperature" and "sdTemperature" are inserted at the "%s" symbols in the SQL string.

# In[31]:


def skewTemperature(df,spark):
    temprddrow = df.select('temperature').rdd #in row(temp=x) format
    temprdd = temprddrow.map(lambda (x) : x["temperature"]) #only numbers
    temp = temprdd.filter(lambda x: x is not None).filter(lambda x: x != "")  #remove None params
    n = float(temp.count())
    sum=temp.sum()
    mean =sum/n
    from math import sqrt
    sd=sqrt(temp.map(lambda x : pow(x-mean,2)).sum()/n)
    skew=n*(temp.map(lambda x:pow(x-mean,3)/pow(sd,3)).sum())/(float(n-1)*float(n-2))
    return skew 


# Kurtosis is the 4th statistical moment, so if you are smart you can make use of the code for skew which is the 3rd statistical moment. Actually only two things are different.

# In[32]:


def kurtosisTemperature(df,spark):
    temprddrow = df.select('temperature').rdd
    temprdd = temprddrow.map(lambda (x) : x["temperature"])
    temp = temprdd.filter(lambda x: x is not None).filter(lambda x: x != "")
    n = float(temp.count())
    sum=temp.sum()
    mean =sum/n
    from math import sqrt
    sd=sqrt(temp.map(lambda x : pow(x-mean,2)).sum()/n)
    kurtosis=temp.map(lambda x:pow(x-mean,4)).sum()/(pow(sd,4)*(n))
    return kurtosis


# Just a hint. This can be solved easily using SQL as well, but as shown in the lecture also using RDDs.

# In[33]:


from pyspark.mllib.stat import Statistics

def correlationTemperatureHardness(df,spark):
    column1 = df.select('temperature').rdd.map(lambda x: x['temperature']).filter(lambda x: x is not None).filter(lambda x: x != '')
    column2 = df.select('hardness').rdd.map(lambda x: x['hardness']).filter(lambda x: x is not None).filter(lambda x: x != '')
    data = column1.zip(column2)
    corr_matrix = Statistics.corr(data)
    
    return corr_matrix[1][0]


# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED
# #axx
# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED

# Now it is time to connect to the cloudant database. Please have a look at the Video "Overview of end-to-end scenario" of Week 2 starting from 6:40 in order to learn how to obtain the credentials for the database. Please paste this credentials as strings into the below code
# 
# ### TODO Please provide your Cloudant credentials here

# In[34]:


### TODO Please provide your Cloudant credentials here by creating a connection to Cloudant and insert the code
credentials_1 = dict()
credentials_1['username'] = '9f7400f1-a47d-4030-b242-da4afd61f53a-bluemix'
credentials_1['password'] = 'c5086d984b9bcac6d7da1f742881db545a7e9889a50d83ab72783e962e5b8231'
credentials_1['custom_url'] = 'https://9f7400f1-a47d-4030-b242-da4afd61f53a-bluemix:c5086d984b9bcac6d7da1f742881db545a7e9889a50d83ab72783e962e5b8231@9f7400f1-a47d-4030-b242-da4afd61f53a-bluemix.cloudant.com'
### Please have a look at the latest video "Connect to Cloudant/CouchDB from ApacheSpark in Watson Studio" on https://www.youtube.com/c/RomeoKienzler
database = "washing" #as long as you didn't change this in the NodeRED flow the database name stays the same


# In[35]:


#Please don't modify this function
def readDataFrameFromCloudant(database):
    cloudantdata=spark.read.load(database, "com.cloudant.spark")

    cloudantdata.createOrReplaceTempView("washing")
    spark.sql("SELECT * from washing").show()
    return cloudantdata


# In[36]:


spark = SparkSession    .builder    .appName("Cloudant Spark SQL Example in Python using temp tables")    .config("cloudant.host",credentials_1['custom_url'].split(':')[2].split('@')[1])    .config("cloudant.username", credentials_1['username'])    .config("cloudant.password",credentials_1['password'])    .config("jsonstore.rdd.partitions", 1)    .getOrCreate()


# In[37]:


df=readDataFrameFromCloudant(database)


# In[38]:


minTemperature(df,spark)


# In[39]:


meanTemperature(df,spark)


# In[40]:


maxTemperature(df,spark)


# In[41]:


sdTemperature(df,spark)


# In[42]:


skewTemperature(df,spark)


# In[43]:


kurtosisTemperature(df,spark)


# In[44]:


correlationTemperatureHardness(df,spark)


# Congratulations, you are done, please download this notebook as python file using the export function and submit is to the gader using the filename "assignment3.1.py"
