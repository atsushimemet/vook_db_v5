#!/usr/bin/env python
# coding: utf-8

# # Import

# In[24]:


# import sys

# sys.path.append("../")
import datetime

from vook_db_v5.local_config import ClientId, pid, sid

aff_id = f"//ck.jp.ap.valuecommerce.com/servlet/referral?vs={sid}&vp={pid}&vc_url="

# import hashlib
import json

import pandas as pd
import requests


# # Global

# In[33]:


REQ_URL_CATE = "https://shopping.yahooapis.jp/ShoppingWebService/V3/itemSearch"


PLATFORM = "Yahoo"
PLATFORM_ID = 2
BRAND = "リーバイス"
BRAND_ID = 1
ITEM = "デニム"
ITEM_ID = 1
LINE = "501"
LINE_ID = 1
KNOWLEDGE = "66前期"
KNOWLEDGE_ID = 1
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

WANT_ITEMS = [
    "id",
    "name",
    "url",
    "price",
    "knowledge_id",
    "platform_id",
    "size_id",
    "created_at",
    "updated_at",
]


# # Main

# In[31]:


query = f"{BRAND} ヴィンテージ {ITEM} {LINE} {KNOWLEDGE}"


# In[36]:


# 現在の日付と時刻を取得 & フォーマットを指定して文字列に変換
datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


# In[37]:


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
                    h["url"],
                    h["price"],
                    KNOWLEDGE_ID,
                    PLATFORM_ID,
                    "",
                    # 現在の日付と時刻を取得 & フォーマットを指定して文字列に変換
                    datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                    # 現在の日付と時刻を取得 & フォーマットを指定して文字列に変換
                    datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                )
            )
        df = pd.DataFrame(l_hit, columns=WANT_ITEMS)
        l_df.append(df)


# In[39]:


products_raw = pd.concat(l_df, axis=0, ignore_index=True)


# In[40]:


products_raw.to_csv(f"./data/output/products_raw.csv", index=False)

