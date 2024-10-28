from dspace_rest_client.client import DSpaceClient
from dspace_rest_client.models import Community, Collection, Item, Bundle, Bitstream
from pprint import pprint
import csv
from datetime import datetime
import pprint
import os
import json

theses_collection_list = [
    {"collection_name": "Arts: Theses and Dissertations", "collection_uuid":"c70844f6-7ceb-4f3d-bfc4-a74c41ba29d6"},
    {"collection_name": "Education: Theses and Dissertations", "collection_uuid":"e1d748fa-8da5-4e6c-9329-be4aaba71a0b"},
    {"collection_name": "Engineering: Theses and Dissertations", "collection_uuid":"154cd67c-752a-4636-aa18-f4eebef5b469"},
    {"collection_name": "Health: Theses and Dissertations", "collection_uuid":"042374b5-d038-47d5-af8f-ba8ca97b467d"},
    {"collection_name": "Law: Theses and Dissertations", "collection_uuid":"cacdac2a-dea5-4565-9c3a-c93324423d97"},
    {"collection_name": "Science: Theses and Dissertations", "collection_uuid":"ccf82095-90b1-41dd-a18e-06ae942f5513"},
    {"collection_name": "Business: Theses and Dissertations", "collection_uuid":"c22568ca-6cc2-41c4-8a48-d988e3713379"}
]

url = 'https://ir.canterbury.ac.nz/server/api'
username = 'anton.angelo@canterbury.ac.nz'
password = '5jnUiRm9hrsWYnW'

def write_pdf_to_file(content, metadata):

    os.mkdir(metadata['pdf_uuid'])
    pdf_to_write = f'{metadata["pdf_uuid"]}/{metadata["pdf_file_name"]}.pdf'
    metadatafile_to_write = f'{metadata["pdf_uuid"]}/metadata.json'
    f = open(pdf_to_write, "wb")
    print(f'writing {pdf_to_write}, {metadatafile_to_write}')
    f.write(content)
    f.close()
    f = open(metadatafile_to_write, "w")
    f.write(json.dumps(metadata))
    f.close



# Instantiate DSpace client
# Note the 'fake_user_agent' setting here -- this will set a string like the following, to get by Cloudfront:
# Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36
# The default is to *not* fake the user agent, and instead use the default of DSpace Python REST Client.
# To specify a custom user agent, set the USER_AGENT env variable and leave/set fake_user_agent as False
d = DSpaceClient(api_endpoint=url, username=username, password=password, fake_user_agent=True)

# Authenticate against the DSpace client
authenticated = d.authenticate()
if not authenticated:
    print('Error logging in! Giving up.')
    exit(1)

    #     # Get all items in this collection - see that the recommended method is a search, scoped to this collection
    #     # (there is no collection/items endpoint, though there is a /mappedItems endpoint, not yet implemented here)
for collections in theses_collection_list:
    item_uuid = collections['collection_uuid']
    print(f"#COLLECTION: {collections['collection_name']}")

    dspace_items = d.search_objects(query='*:*', scope=item_uuid, dso_type='item')
    for dspace_item in dspace_items:
        metadata_dict ={}
        metadata_dict['author'] = dspace_item.metadata['dc.contributor.author'][0]['value']
        metadata_dict['title'] = dspace_item.metadata['dc.title'][0]['value']
        metadata_dict['date'] = dspace_item.metadata['dc.date.issued'][0]['value']
        for uri_dict in dspace_item.metadata['dc.identifier.uri']:
            if 'doi' in uri_dict['value']:
                metadata_dict['doi'] = uri_dict['value']
        # metadata_dict['rights'] = dspace_item.metadata['dc.rights'][0]['value']


        # Get all bundles in this item
        bundles = d.get_bundles(parent=dspace_item)
        for bundle in bundles:
            if 'ORIGINAL' in bundle.name:
                #print(f'{bundle.name} ({bundle.uuid}')
                # Get all bitstreams in this bundle
                bitstreams = d.get_bitstreams(bundle=bundle)
                for bitstream in bitstreams:
                    if 'pdf' in bitstream.name:
                        print(f'{bitstream.name} ({bitstream.uuid}')
                        metadata_dict['pdf_file_name'] = bitstream.name
                        metadata_dict['pdf_uuid'] = bitstream.uuid
                        pprint.pp(metadata_dict)

                        # Download this bitstream
                        r = d.download_bitstream(bitstream.uuid)
                        if r is not None and r.headers is not None:
                            print(f'\tHeaders (server info, not calculated locally)\n\tmd5: {r.headers.get("ETag")}\n'
                            f'\tformat: {r.headers.get("Content-Type")}\n\tlength: {r.headers.get("Content-Length")}\n'
                            f'\tLOCAL LEN(): {len(r.content)}')

                    # Uncomment the below to get the binary data in content and then do something with
                    #it like
                    # print, or write to file, etc. You want to use the 'content' property of the
                    #response object
                    #

                            write_pdf_to_file(r.content, metadata_dict)
