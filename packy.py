import json
import requests
import re
import pandas as pd
import redis
import os

from twilio.rest import Client as twilio

if not os.environ.get("PRODUCTION"):
    from dotenv import load_dotenv
    load_dotenv(verbose=True)

REDIS_PREFIX = 'packages'
APT = os.getenv('APT')
API_URL = os.getenv('API_URL')

def stored_counts(key):
    stored_bytes = redis_server().hgetall(f"{REDIS_PREFIX}-{key}")
    if not bool(stored_bytes):
        return {}
    return { key.decode(): int(val.decode()) for (key, val) in stored_bytes.items() }

def set_counts(key, counts):
    if not bool(counts):
        reset_redis(key)
        return
    return redis_server().hmset(f"{REDIS_PREFIX}-{key}", counts)

def reset_redis(key):
    redis_server().delete(f"{REDIS_PREFIX}-{key}")

def redis_server():
    return redis.from_url(os.environ.get("REDIS_URL"))

def fetch():
    response = requests.get(API_URL)

    if response.status_code == 200:
        return json.loads(response.content.decode('utf-8'))
    else:
        return None

def unit_packages(number, package_listing):
    units = package_listing["packages"]
    return next((unit for unit in units if re.search(f"{number}\s*", unit['unitCode'])), None)

def package_summary(package_counts):
    m = map(lambda c: ({"num": c['count'], "vendor": c['vendor']}), package_counts['packageCounts'])
    df = pd.DataFrame(m)
    grouped = df.groupby('vendor')['num'].apply(list).to_dict()
    return {k: int(v[0]) for k, v in grouped.items()}

def my_summary(all_packages):
    mine = unit_packages(APT, all_packages)
    if mine is not None:
        return package_summary(mine)
    else:
        return {}

def notify(id, additions, counts):
    body = f"Hi, it's Charlie!\n\nYou've got new packages:\n{additions}\n\nAll packages waiting:\n{counts}"
    send_notifications(body)

def send_notifications(body):
    to_numbers = os.getenv('TWILIO_TO').split(',')
    for number in to_numbers:
        send_message(number, body)

def send_message(to, body):
    account_sid = os.getenv('TWILIO_SID')
    auth_token  = os.getenv('TWILIO_TOKEN')

    client = twilio(account_sid, auth_token)

    message = client.messages.create(
        to=to, 
        from_=os.getenv('TWILIO_FROM'),
        body=body)

    print(f"Notified: {message.sid}")

def difference(a, b):
    counts = { k: a[k] - b.get(k, 0) for k,v in a.items() }
    return dict(filter(lambda item: item[1] > 0, counts.items()))

def real_response(response):
    return isinstance(response, dict) and ('packages' in response) and len(response['packages']) > 0

def main():
    previous_counts = stored_counts(APT)
    all_packages = fetch()
    
    if not real_response(all_packages):
        print("API probably down")
        return
    
    current_counts = my_summary(all_packages)
    set_counts(APT, current_counts)
    additions = difference(current_counts, previous_counts)

    if bool(additions):
        notify(APT, additions, current_counts)
    
    print(additions, current_counts)

# import pdb; pdb.set_trace()

main()

