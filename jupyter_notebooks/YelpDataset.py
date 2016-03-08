
# coding: utf-8

# In[ ]:

BIZPATH='../montreal_subset/montreal_business.json'
USERPATH='../montreal_subset/montreal_users_who_reviewed.json'
REVPATH='../montreal_subset/montreal_reviews.json'
CHECKINPATH='../montreal_subset/montreal_checkin.json'
Test = ""


# In[ ]:

import json


# In[ ]:

bizrecords = [json.loads(line) for line in open(BIZPATH)]
userrecords = [json.loads(line) for line in open(USERPATH)]
revrecords = [json.loads(line) for line in open(REVPATH)]
checkinrecords = [json.loads(line) for line in open(CHECKINPATH)]


# In[ ]:

bizrecords[0]


# In[ ]:

userrecords[0]


# In[ ]:

revrecords[0]


# In[ ]:

checkinrecords[0]


# In[ ]:

from pandas import DataFrame, Series


# In[ ]:

import pandas as pd; import numpy as np


# In[ ]:

bizframe = DataFrame(bizrecords)
userframe = DataFrame(userrecords)
revframe = DataFrame(revrecords)
checkinframe = DataFrame(checkinrecords)


# In[ ]:

users_df = DataFrame(userrecords, columns=['user_id','yelping_since','review_count', 'average_stars', 'variance_in_rating'])


# In[ ]:

users_df.head(n=10)


# In[ ]:

bizframe_sub = DataFrame(bizrecords, columns=['business_id', 'name', 'categories', 'review_count', 'stars'])


# In[ ]:

bizframe_sub.head(n=10)


# In[ ]:

bsort = bizframe_sub.sort_values(by='review_count', ascending=False)


# In[ ]:

bsort[:10]


# In[ ]:

bizframe_sub.corr()


# In[ ]:

top_biz = bizframe_sub.ix[bizframe_sub.stars == 5.0]


# In[ ]:

tb = top_biz.sort_values(by='review_count', ascending=False)


# In[ ]:

tb[:10]


# In[ ]:

revframe


# In[ ]:

rev_star_counts = revframe['stars'].value_counts()


# In[ ]:

rev_star_counts[:10]


# In[ ]:

user_avg_star_counts = userframe['average_stars'].value_counts()


# In[ ]:

user_avg_star_counts[:]


# In[ ]:

biz_star_counts = bizframe['stars'].value_counts()


# In[ ]:

biz_star_counts


# In[ ]:

bizframe['stars'].describe()


# In[ ]:

userframe['review_count'].describe()


# In[ ]:

userframe['review_count'].skew()


# In[ ]:

revframe['stars'].describe()


# In[ ]:

revframe['stars'].median()


# In[ ]:

rev_user_frame = pd.merge(revframe, userframe, on='user_id')


# In[ ]:

rev_user_frame


# In[ ]:

rev_user_sample = rev_user_frame.take(np.random.permutation(len(rev_user_frame))[:50])


# In[ ]:

rev_user_sample.describe()


# In[ ]:

rev_user_sample.stack()


# In[ ]:

rev_user_sample.unstack()


# In[ ]:

rev_user_sample.corr()


# In[ ]:

group = revframe.groupby('user_id').size()


# In[ ]:

group.sort_values(ascending=False)

