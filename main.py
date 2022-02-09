import requests
import json
import argparse
import logging
from azure.storage.blob import BlobServiceClient, ContentSettings
import json
from multiprocessing.pool import ThreadPool


# Define next chunk to process in the batch
def get_next_chunk(current_val):    
    return chr(ord(current_val) + 1), chr(ord(current_val) + 1) , chr(ord(current_val) + 2)


# Get all documents in a batch [low_b, high_b]
def get_all_docs(low_b, high_b, src_endpoint, src_headers, filter_by= "Id"):

    documents = []

    searchstring = f"&$filter={filter_by} gt '{low_b}' and {filter_by} le '{high_b}'&$count=true"
    url = src_endpoint + "indexes/" + src_index +"/docs" + api_version + searchstring
    response  = requests.get(url, headers=src_headers, json=searchstring)
    query = response.json()

    if query.get('value') != None:

        for doc in query.get('value'):
            documents.append(doc)

            # Continue if needed
        while('@odata.nextLink' in query.keys()):
            next_link = query['@odata.nextLink']
            #print(next_link)

            response = requests.get(next_link, headers=src_headers)
            query = response.json()

            for doc in query.get('value'):
                documents.append(doc)

        #print(query)

    return documents


def push_docs(all_documents, dst_endpoint, dst_headers):
    # Push data
    batch_index = 0
    while batch_index * 50 < len(all_documents):
        batch_start = batch_index * 50
        batch_end = (batch_index + 1) * 50 if (batch_index + 1) * 50 < len(all_documents) else len(all_documents)
        logging.info(f"Pushing batch #{batch_index} [{batch_start},{batch_end}]")
        push_batch(all_documents[batch_start:batch_end], dst_endpoint, dst_headers)
        batch_index += 1

        # print(index_content)


def push_doc_on_blob(doc):
    # Push data on blob
    blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{doc[doc_key]}.json")
    blob_client.upload_blob(json.dumps(doc), content_settings = ContentSettings(content_type='application/json'))


def push_docs_on_blob(all_documents):
    with ThreadPool(processes=int(50)) as pool:
      return pool.map(push_doc_on_blob, all_documents)


def push_batch(batch_documents, dst_endpoint, dst_headers):

    search_docs = {
        "value" : []
    }

    for d in batch_documents:
        del d['@search.score'] 
        d['@search.action'] = 'mergeOrUpload'
        search_docs['value'].append(d)


    # search_docs['value'][0]

    url = dst_endpoint + "indexes/" + dst_index + '/docs/index' + api_version
    response = requests.post(url, headers = dst_headers, json = search_docs)
    index_content = response.json()
    logging.info(response.status_code)


# MAIN
if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("debug.log"),
            logging.StreamHandler()
        ]
    )
    parser = argparse.ArgumentParser(description='Input parameter for azure-search-copy-index')
    parser.add_argument('--src_service', dest="src_service", required=True,
                        help='Azure Cognitive Search SOURCE Service')
    parser.add_argument('--dst_service', dest="dst_service",
                        help='Azure Cognitive Search DESTINATION Service')
    parser.add_argument('--src_service_key', dest="src_service_key", required=True,
                        help='Azure Cognitive Search SOURCE Service KEY')
    parser.add_argument('--dst_service_key', dest="dst_service_key",
                        help='Azure Cognitive Search DESTINATION Service KEY')
    parser.add_argument('--src_index', dest="src_index", required=True,
                        help='Azure Cognitive Search SOURCE Index')
    parser.add_argument('--dst_index', dest="dst_index", required=False,
                        help='Azure Cognitive Search DESTINATION Index')                       
    parser.add_argument('--filter_by', dest="filter_by", required=True,
                        help='Azure Cognitive Search DESTINATION Index')      
    parser.add_argument('--dump_on_blob', dest="blob_connection_string", required=False,
                        help="Azure Blob Storage Connection String for index dump")
    parser.add_argument('--container_name', dest="container_name", required=False,
                        help="Azure Blob Storage container for index dump")

    args = parser.parse_args()

    src_service = args.src_service
    dst_service = src_service if args.dst_service is None else args.dst_service

    src_service_key = args.src_service_key
    dst_service_key = src_service_key if args.dst_service_key is None else args.dst_service_key

    src_index = args.src_index
    dst_index = args.dst_index

    filter_by = args.filter_by

    blob_connection_string = args.blob_connection_string
    container_name = args.container_name

    logging.info(f"Copying :\n\tSOURCE SERVICE: {src_service} INDEX: {src_index}\n\tDESTINATION SERVICE {dst_service} INDEX: {dst_index}\n\tFiltering by: {filter_by}")

    src_endpoint = 'https://' + src_service + '.search.windows.net/'
    dst_endpoint = 'https://' + dst_service + '.search.windows.net/'

    api_version = '?api-version=2019-05-06'
    src_headers = {'Content-Type': 'application/json',
            'api-key': src_service_key } 
    dst_headers = {'Content-Type': 'application/json',
            'api-key': dst_service_key } 


    searchstring = '&search=*&$count=true'

    url = src_endpoint + "indexes/" + src_index +"/docs" + api_version + searchstring
    response  = requests.get(url, headers=src_headers, json=searchstring)
    query = response.json()
    #print(query)

    docCount = query['@odata.count']

    logging.info(f"Copying {docCount} documents") 

    # Get and push docs

    val = '/'
    while (ord(val) <= 123): #123
        val, low_b, high_b = get_next_chunk(val)
        logging.info(f" Processing interval : [{low_b},{high_b}]")
        # val = chr(ord(val) + 1)
        documents = []
        documents = get_all_docs(low_b, high_b, src_endpoint, src_headers, filter_by=filter_by)
        logging.info(len(documents))
        if dst_index != None:
            push_docs(documents, dst_endpoint, dst_headers)
        if blob_connection_string != None:
            doc_key = filter_by
            push_docs_on_blob(documents)


    # Check
    if dst_index != None:

        searchstring = '&search=*&$count=true'#&$orderby=metadata_storage_last_modified desc'

        url = dst_endpoint + "indexes/" + dst_index +"/docs" + api_version + searchstring
        response  = requests.get(url, headers=dst_headers, json=searchstring)
        query = response.json()


        docCount = query['@odata.count']

        logging.info(f"Copied {docCount} documents")
