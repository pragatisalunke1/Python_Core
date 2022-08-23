#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession


# In[2]:


spark=SparkSession.builder.appName('DataFrame').getOrCreate()


# In[3]:


spark


# In[52]:


##Read the data set
spark.read.option('header','true').csv('test1.csv').show()


# In[53]:


df_pyspark=spark.read.option('header','true').csv('test1.csv',inferSchema=True)


# In[54]:


df_pyspark.printSchema()


# In[55]:


df_pyspark=spark.read.csv('test1.csv',header=True,inferSchema=True)
df_pyspark.show()


# In[56]:


df_pyspark.printSchema()


# In[57]:


type(df_pyspark)


# In[58]:


df_pyspark.columns


# In[59]:


df_pyspark.head(3)


# In[60]:


df_pyspark.show()


# In[61]:


df_pyspark.select('Name').show()


# In[62]:


df_pyspark.select('Name')


# In[63]:


type(df_pyspark)


# In[64]:


df_pyspark.select(['Name','Experience']).show()


# In[65]:


df_pyspark.select(['Name','Experience'])


# In[66]:


type(df_pyspark)


# In[67]:


df_pyspark.describe().show()


# In[68]:


### Adding Columns in data frame
df_pyspark=df_pyspark.withColumn('Experience After 2 year',df_pyspark['Experience']+2)


# In[69]:


df_pyspark.show()


# In[70]:


type(df_pyspark)


# In[71]:


##droping the column
df_pyspark=df_pyspark.drop('Experience After 2 year')


# In[72]:


df_pyspark.show()


# In[73]:


### Rename the columns
df_pyspark.withColumnRenamed('Name','New Name').show()



# In[49]:





# In[ ]:




