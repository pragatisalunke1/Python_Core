#!/usr/bin/env python
# coding: utf-8

# In[22]:


from pyspark.sql import SparkSession


# In[23]:


spark=SparkSession.builder.appName('Practice DE').getOrCreate()


# In[46]:


##Read the data set
##spark.read.option('header','true').csv('test2.csv').show()
df_pyspark=spark.read.csv('test2.csv',header=True,inferSchema=True)


# In[47]:


df_pyspark.show()


# In[48]:


df_pyspark.printSchema()


# In[49]:


##drop the columns
df_pyspark.drop('Name').show()



# In[50]:


df_pyspark.show()



# In[51]:


df_pyspark.na.drop().show()


# In[52]:


### all==how
df_pyspark.na.drop(how='all').show()


# In[53]:


### any==how
df_pyspark.na.drop(how="any").show()


# In[58]:


##threshold
df_pyspark.na.drop(how="any",thresh=3).show()


# In[61]:


##Subset
df_pyspark.na.drop(how="any",subset=['Experience']).show()



# In[68]:


### Filling the Missing Value
df_pyspark.na.fill(0,['Experience','age']).show()


# In[71]:


from pyspark.ml.feature import Imputer

imputer = Imputer(
    inputCols=['Age', 'Experience', 'Salary'], 
    outputCols=["{}_imputed".format(c) for c in ['Age', 'Experience', 'Salary']]
    ).setStrategy("median")


# In[72]:


# Add imputation cols to df
imputer.fit(df_pyspark).transform(df_pyspark).show()


# In[ ]:




