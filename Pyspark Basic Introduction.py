#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip install pyspark')


# In[2]:


import pyspark


# In[31]:


import pandas as pd
pd.read_csv('test1.csv')
#type(pd.read_csv('test1.csv'))


# In[32]:


from pyspark.sql import SparkSession


# In[33]:


spark=SparkSession.builder.appName('Practice').getOrCreate()


# In[34]:


spark


# In[35]:


df_pyspark=spark.read.csv('test1.csv')


# In[36]:


#df_pyspark


# In[37]:


#df_pyspark.show()


# In[38]:


#spark.read.option('header','true').csv('test1.csv')


# In[47]:


#df_pyspark=spark.read.option('header','true').csv('test1.csv').show() --it is showing nonetype because of show
df_pyspark=spark.read.option('header','true').csv('test1.csv')


# In[48]:


type(df_pyspark)


# In[49]:


df_pyspark.head(2)


# In[50]:


df_pyspark.printSchema()


# In[ ]:




