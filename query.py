import pandas as pd
from elasticsearch import Elasticsearch, exceptions
es = Elasticsearch(
    ['http://localhost:9200'],  
    basic_auth=('elastic', 'tRdmjs52knn3IU7EZAL6')  
)

try:
    data = pd.read_csv('C:/Users/Admin/OneDrive/Desktop/drive/Employee Sample Data 1.csv', encoding='latin1')
except FileNotFoundError as e:
    print(f"Error: {e}")
    exit(1)  
except Exception as e:
    print(f"An error occurred while reading the CSV: {e}")
    exit(1)
print("Columns in DataFrame:", data.columns)
data.fillna({
    'Full Name': '',
    'Job Title': '',
    'Department': '',
    'Business Unit': '',
    'Gender': '',
    'Ethnicity': '',
    'Age': 0,
    'Hire Date': '',
    'Annual Salary': 0.0,
    'Bonus %': '0%',
    'Country': '',
    'City': '',
    'Exit Date': ''
}, inplace=True)
data['Annual Salary'] = data['Annual Salary'].replace({'\$': '', ',': ''}, regex=True).astype(float)
index_name = 'employee_index'
if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name)
for index, document in data.iterrows():
    try:

        doc_dict = document.where(pd.notnull(document), None).to_dict()
        es.index(index=index_name, id=document['Employee ID'], body=doc_dict)
        print(f"Indexed document {document['Employee ID']} successfully.")
    except exceptions.BadRequestError as e:
        print(f"Error indexing document {document['Employee ID']}: {e}")
    except exceptions.AuthenticationException as e:
        print(f"Authentication error: {e}")
    except Exception as e:
        print(f"An error occurred while indexing document {document['Employee ID']}: {e}")
try:
    response = es.search(index=index_name, body={
        "query": {
            "match_all": {}
        }
    })

    print("Search Results:")
    for hit in response['hits']['hits']:
        print(hit["_source"])
except Exception as e:
    print(f"An error occurred during the search: {e}")
