#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark import SparkContext, SparkConf


# In[2]:


conf = SparkConf().setAppName("Youtube_Demo").setMaster("local")
sc = SparkContext(conf=conf)


# In[6]:


data = sc.textFile('books.csv')


# In[7]:


type(data)


# In[8]:


data.top(2)


# In[9]:


data.collect()


# In[10]:


data.take(2)


# In[11]:


for line in data.take(5):
    print(line)


# In[12]:


data.first()


# In[13]:


oneRecord = data.first()
columns = oneRecord.split(',')
columns


# In[14]:


import pyspark as ps
spark = ps.sql.SparkSession.builder.master("local").appName("FirstApp").getOrCreate() 


# In[15]:


spark = ps.sql.SparkSession.builder.getOrCreate()


# In[16]:


type(spark)


# In[17]:


# Importing books dataset as Spark dataframe
# header=True, Wanted to keep heard with dataframe as most of logic are based on DataFrame Header
# inferSchema=True To auto infer dataset as per the data given as ALS needs only number

books_df = spark.read.csv('books.csv', header=True, inferSchema=True) 

# Lets see schema (Column deatils) of this book_df
books_df.printSchema()


# In[18]:


type(books_df)


# In[19]:


len(books_df.columns)


# In[21]:


ratings_df = spark.read.csv('ratings.csv', header=True, inferSchema=True) 


# In[22]:


type(ratings_df)


# In[23]:


ratings_df.count()


# In[24]:


ratings_df.printSchema()


# In[25]:


ratings_df.first()


# In[26]:


ratings_df.show(5)


# In[27]:


ratings_df.head(5)


# In[28]:


ratings_df.select("book_id","rating").show(5)


# In[29]:


ratings_df.filter("rating <= 3").show(5)


# In[30]:


ratings_df.select("book_id","rating").filter("rating <= 3").show(5)


# In[31]:


ratings_df.count()


# In[32]:


print(f"Total number of Ratings Records : {ratings_df.count()}")


# In[33]:


unique_user_count = ratings_df.select("user_id").distinct().count()
unique_user_count


# In[34]:


# Count of Rating whose value is 3 or less than 3.
book_rating_less_or_three_count = ratings_df.filter("rating <= 3").count()
book_rating_less_or_three_count


# In[35]:


ratings_df.describe('book_id').show()


# In[36]:


ratings_df.describe('book_id','rating').show()


# In[37]:


ratings_df.count()


# In[38]:


aaa = ratings_df.dropDuplicates()


# In[39]:


aaa.count()


# In[40]:


## Drop all the rows with null values.
rating_without_null = ratings_df.dropna().count()


# In[41]:


rating_without_null


# In[42]:


ratings_df.dropna('any').count() # drop a row if it contains any nulls


# In[43]:


ratings_df.dropna('all').count() # drop a row if it contains any nulls


# In[44]:


# Maximum value of any column
ratings_df.agg({'rating':'max'}).show()


# In[45]:


ratings_df.groupby("rating").count().toPandas()


# In[46]:


ratings_df.groupby("rating").count().show()


# In[47]:


# Join two csv dataset
ratings_df.join(books_df, books_df.book_id == ratings_df.book_id).select("user_id","title").show(5)


# In[48]:


ratings_df.orderBy("rating").show(5)


# In[49]:


ratings_df.orderBy(ratings_df.rating.desc()).show(5)


# In[50]:


ratings_df.orderBy("rating","book_id").show(115)


# In[51]:


# Change the value of an existing columns
ratings_df.withColumn("rating", ratings_df.rating*10).show(5)


# In[52]:


# Add new columns
new_dataset = ratings_df.withColumn("rating_ten", ratings_df.rating*10)
new_dataset.show(5)


# In[53]:


ratings_df.show(5)


# In[54]:


# How to drop a column
ratings_df.drop('rating').show(5)


# In[55]:


ratings_df.show()


# In[ ]:




