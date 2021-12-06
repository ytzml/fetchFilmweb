from bs4 import BeautifulSoup
import requests
import pandas as pd
from io import StringIO
import boto3
from datetime import datetime


req = requests.request("GET", "https://www.filmweb.pl/ranking/wantToSee/next6monthsPoland")
html = req.content.decode()
soup = BeautifulSoup(html, features="html.parser")
rankingElems = soup.find_all("div", {"class": "rankingType__card"})
ranking_data = []
for elem in rankingElems:
    title = elem.find_all("a")[0].text
    votes = int(''.join(elem.find_all("span", {"class": "rankingType__rate--count"})[0].text.split()[:-2]))
    ranking_data.append([title, votes])
ranking_df = pd.DataFrame(columns=["title", "votes"], data=ranking_data)
print(ranking_df)
out_buffer = StringIO()
ranking_df.to_csv(out_buffer)
s3_client = boto3.client('s3')
s3_client.put_object(Bucket="jama-data",
                         Key=f'filmweb-ranking/year={datetime.now().year}/month={datetime.now().month}/day={datetime.now().day}/data.csv',
                         Body=out_buffer.getvalue())

