import csv
import pprint
import requests
import json
import os.path
import glob
import pandas, re, numpy as np

# create account_subs.tsv
if not os.path.isfile('account_subs.tsv'):
	log=pandas.read_csv('iapi_subscription.txt', sep='\t', names=['ts', 'level', 'line', 'msg'], index_col='ts')
	log['userid'] = log.msg.str.replace('.*user/(\d+)/webhook.*', lambda m: m.group(1)).apply(int)
	log['method'] = log.msg.str.replace('IAPI (\w+) https.*', lambda m: m.group(1))
	log['msg_json'] = log.msg.str.replace('.*webhook(?:/\d+)? 200: (.*)', lambda m: m.group(1))
	def safejson(x):
	    try:
	        return json.loads(x)
	    except (ValueError, KeyError):
	        return None
	v = np.vectorize(safejson)
	log['req'] = v(log['msg_json'])
	log=log.join(log.req.apply(pandas.Series))
	uuz = log[(log.zone_id.isnull())][['userid', 'zone_id', 'action', 'prev_sub_label', 'rate_plan', 'component_values']]
	uuz['component_values'] = uuz['component_values'].apply(json.dumps)
	uuz.to_csv(index=False, path_or_buf='accounts_subs.tsv', sep='\t')