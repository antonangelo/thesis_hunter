import requests
import thefuzz
import json
from dspace_rest_client.client import DSpaceClient
from dspace_rest_client.models import Community, Collection, Item, Bundle, Bitstream
from pprint import pprint
import csv
from datetime import datetime
import pprint
import os
import json
import config

import thefuzz.fuzz

def find_similarity(title, author, doi):
    google_url = "https://www.googleapis.com/books/v1/volumes"
    payload ={
    'key' : config.google_api_key,
    'orderBy':'relevance',
    'q': title + ' ' + author
    }
    r = requests.get(google_url, params=payload)
    #print(r.url)
    data_dict = r.json()
    print(data_dict)
    if data_dict['totalItems'] > 0:
        for item in data_dict['items']:
            #print(f"found {data_dict['totalItems']} in google")
            similarity_ratio = thefuzz.fuzz.partial_ratio(title, item['volumeInfo']['title'])
            if similarity_ratio > 80:
                output_list =  [title,author,similarity_ratio,item['volumeInfo']['title'],item['saleInfo']['saleability'],item['volumeInfo']['previewLink'],doi]
                return output_list


theses_collection_list = [
    {"collection_name": "Arts: Theses and Dissertations", "collection_uuid":"c70844f6-7ceb-4f3d-bfc4-a74c41ba29d6"},
    {"collection_name": "Education: Theses and Dissertations", "collection_uuid":"e1d748fa-8da5-4e6c-9329-be4aaba71a0b"},
    {"collection_name": "Engineering: Theses and Dissertations", "collection_uuid":"154cd67c-752a-4636-aa18-f4eebef5b469"},
    {"collection_name": "Health: Theses and Dissertations", "collection_uuid":"042374b5-d038-47d5-af8f-ba8ca97b467d"},
    {"collection_name": "Law: Theses and Dissertations", "collection_uuid":"cacdac2a-dea5-4565-9c3a-c93324423d97"},
    {"collection_name": "Science: Theses and Dissertations", "collection_uuid":"ccf82095-90b1-41dd-a18e-06ae942f5513"},
    {"collection_name": "Business: Theses and Dissertations", "collection_uuid":"c22568ca-6cc2-41c4-8a48-d988e3713379"}
]

repository_url = config.repository_url
username = config.username
password = config.password


# Instantiate DSpace client
# Note the 'fake_user_agent' setting here -- this will set a string like the following, to get by Cloudfront:
# Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36
# The default is to *not* fake the user agent, and instead use the default of DSpace Python REST Client.
# To specify a custom user agent, set the USER_AGENT env variable and leave/set fake_user_agent as False
d = DSpaceClient(api_endpoint=repository_url, username=username, password=password, fake_user_agent=True)

# Authenticate against the DSpace client
authenticated = d.authenticate()
if not authenticated:
    print('Error logging in! Giving up.')
    exit(1)

    #     # Get all items in this collection - see that the recommended method is a search, scoped to this collection
    #     # (there is no collection/items endpoint, though there is a /mappedItems endpoint, not yet implemented here)

with open ("results.csv", 'w', newline='', encoding="UTF8") as csvresults:
    resultswriter = csv.writer(csvresults, quoting=csv.QUOTE_ALL)
    
    for collections in theses_collection_list:
        item_uuid = collections['collection_uuid']
        #print(f"#COLLECTION: {collections['collection_name']}")
        page_number = 0
        floop = 2
        while floop > 0:
            dspace_items = d.search_objects(query='*:*', scope=item_uuid, dso_type='item',  page=page_number)
            floop = len(dspace_items)
            page_number = page_number + 1

            for dspace_item in dspace_items:
                
                author = dspace_item.metadata['dc.contributor.author'][0]['value']
                title= dspace_item.metadata['dc.title'][0]['value']
                title = title.replace("\n", " ")
                date = dspace_item.metadata['dc.date.issued'][0]['value']
                for uri_dict in dspace_item.metadata['dc.identifier.uri']:
                    if 'doi' in uri_dict['value']:
                        doi = uri_dict['value']
                print(f"Searching for {collections['collection_name']}/{page_number}: {author}, {title},{date},{doi}")
                output_list = find_similarity(title, author, doi)
                print(output_list)
                if output_list is not None:
                    output_list.append(collections['collection_name'])
                    resultswriter.writerow(output_list)
                    csvresults.flush()