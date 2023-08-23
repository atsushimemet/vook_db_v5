#!/usr/bin/env python
# coding: utf-8

# # Import

# In[13]:


import sys

sys.path.append("../")
import datetime
import hashlib
import json

import pandas as pd
import requests

from vook_db_v4.config import ClientId, aff_id, pid, sid


# # Global

# In[25]:


REQ_URL_CATE = "https://shopping.yahooapis.jp/ShoppingWebService/V3/itemSearch"

WANT_ITEMS = [
    "itemCode",
    "itemName",
    "itemPrice",
    "itemUrl",
]
PLATFORM_ID = 2
BRAND = "リーバイス"
BRAND_ID = 1
ITEM = "デニム"
ITEM_ID = 1
LINE = "66前期"
LINE_ID = 1
START_AGE = 1974
END_AGE = 1977
AGES_ID = 1
RUN_TIME = datetime.datetime.today().strftime("%Y%m%d_%H%M%S")
INFO_GET_DATE = datetime.datetime.today()
TABLE_COLUMNS = [
    "product_id",
    "product_name",
    "platform_id",
    "ages_id",
    "brand_id",
    "item_id",
    "line_id",
    "price",
    "info_get_date",
    "status",
]


# # Main

# In[3]:


query = f"{BRAND} ヴィンテージ {ITEM} {LINE}"


# In[4]:


params = {
    "appid": ClientId,
    "output": "json",
    "query": query,
    "sort": "-price",
    "affiliate_id": aff_id,
    "affiliate_type": "vc",
    "results": 100,  # NOTE: 100個ずつしか取得できない。
}

start_num = 1
step = 100
max_products = 1000
l_df = []
for inc in range(0, max_products, step):
    params["start"] = start_num + inc
    df = pd.DataFrame(columns=WANT_ITEMS)
    res = requests.get(url=REQ_URL_CATE, params=params)
    res_cd = res.status_code
    if res_cd != 200:
        print(f"Bad request")
        break
    else:
        res = json.loads(res.text)
        if len(res["hits"]) == 0:
            print("If the number of returned items is 0, the loop ends.")
        print(f"Get Data")
        l_hit = []
        for h in res["hits"]:
            l_hit.append(
                (
                    h["index"],
                    h["name"],
                    h["price"],
                    h["url"],
                )
            )
        df = pd.DataFrame(l_hit, columns=WANT_ITEMS)
        l_df.append(df)


# In[7]:


lines_raw = pd.concat(l_df, axis=0, ignore_index=True)
lines_raw


# In[9]:


lines_raw.to_csv(
    f"../data/output/{RUN_TIME}_lines_raw_{BRAND}_{ITEM}_{LINE}.csv", index=False
)


# In[26]:


product_id_list = []
product_name_list = []
platform_id_list = []
ages_id_list = []
brand_id_list = []
item_id_list = []
line_id_list = []
price_list = []
info_get_date_list = []
info_published_date_list = []
status_list = []
for _, row in lines_raw.iterrows():
    product_id_list.append(hashlib.md5(str(row["itemCode"]).encode()).hexdigest())
    product_name_list.append(row["itemName"])
    price_list.append(row["itemPrice"])
    # info_published_date_list = []  # TODO
    # status_list = []  # TODO
tmp_cols = ["product_id", "product_name", "price"]
tmp_df = pd.DataFrame(
    zip(product_id_list, product_name_list, price_list),
    columns=tmp_cols,
)
tmp_df["platform_id"] = PLATFORM_ID
tmp_df["ages_id"] = AGES_ID
tmp_df["brand_id"] = BRAND_ID
tmp_df["item_id"] = ITEM_ID
tmp_df["line_id"] = LINE_ID
tmp_df["info_get_date"] = INFO_GET_DATE
tmp_df["status"] = ""
tmp_df = tmp_df[TABLE_COLUMNS]
lines = tmp_df.copy()


# In[29]:


lines.dtypes


# In[30]:


lines.head()


# In[31]:


lines.to_csv(f"../data/output/{RUN_TIME}_products.csv", index=False)

