#!/usr/bin/env python
# coding: utf-8

# In[24]:


from pyspark import SparkContext, SparkConf


# In[25]:


#First way to create SparkContext
conf = SparkConf().setAppName("Youtube_Demo").setMaster("local")
sc = SparkContext(conf=conf)


# In[26]:


sc.getConf().getAll()


# In[27]:


sc.stop()


# In[28]:


#Second way, Create a SparkContext that loads settings

sc = SparkContext()


# In[29]:


sc.getConf().getAll()


# In[37]:


sc.stop()


# In[38]:


# third way to combining both 
sc = SparkContext("local", "First App")


# In[39]:


from IPython.display import Image
Image(filename='sparkarchitecture.png')


# In[40]:


Image(filename='RDD_Action_Transformation.jpeg')


# In[41]:


#Create RDD and their Basic Actions
names = sc.parallelize(['Pragati','Vikas','Salunke','Nikhil','Snehal','Preyas','Ashlesha','Satyam','Ankita','Akshy','ajay','sayli','priyanka'])


# In[42]:


type(names)


# In[43]:


names.collect()


# In[48]:


names.countByValue()


# In[49]:


def f(x): print(x)
a=sc.parallelize([1, 2, 3, 4, 5]).foreach(lambda x : print(x))


# In[50]:


type(a)


# In[51]:


a=sc.parallelize([(1,2),(2,4)])


# In[52]:


a.countByValue()


# In[53]:


names.collect()


# In[54]:


names.take(5)


# In[56]:


employees = sc.textFile("employees.txt")


# In[57]:


type(employees)


# In[58]:


employees.collect()


# In[59]:


employees.first()


# In[61]:


employees.count()


# In[62]:


employees.top(3)


# In[63]:


employees.distinct().count()


# In[64]:


#Taking number example for better clarity
num = sc.parallelize([5,5,4,3,2,9,2],9)
num.collect()


# In[65]:


num.take(4)


# In[66]:


num.countByValue()


# In[67]:


type(num)


# In[68]:


num.glom().collect()


# In[69]:


type(num.glom())


# In[70]:


num.max()


# In[71]:


num.min()


# In[72]:


num.mean()


# In[73]:


num.collect()


# In[74]:


num.reduce(lambda a,b: a+b)


# In[75]:


num.reduce(lambda a,b: a*b)


# In[76]:


num.reduce(lambda a,b: a-b)


# In[77]:


num.reduce(lambda x,y: x if x > y else y)


# In[78]:


def myfun(a,b):
    return a*2 + b*2


# In[79]:


num.reduce(myfun)


# In[80]:


num.collect()


# In[81]:


num.takeOrdered(3)


# In[82]:


# fold: the initial value for the accumulated result of each partition for the op operator, 
# and also the initial value for the combine results from different partitions


# In[83]:


num = sc.parallelize([5,5,4,3,2,9,2],2)
num.collect()


# In[84]:


num.glom().collect()


# In[85]:


num.reduce(lambda a,b: a+b)


# In[86]:


num.reduce(lambda a,b: a*b)


# In[87]:


num.fold(2,lambda a,b:a+b)


# In[88]:


num.fold(2,lambda a,b : a*b )


# In[89]:


from operator import add
b=sc.parallelize([1, 2, 3, 4, 5])
b.fold(1, add)


# In[90]:


from operator import add,mul
num3 = sc.parallelize([5,5,4,3,2,9,2]).fold(10,mul)
num3


# In[91]:


b = sc.parallelize(range(1,10))


# In[92]:


b.collect()


# In[ ]:




