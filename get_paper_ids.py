import requests
import csv
import json 

field='data-querying'
api_key = 'pAt0JejQAw848v1xD4xEo27jn5nWxmDc4jjAH69D'


# API Call to extract paper ids
def get_paper_info(paper_id, api_key):
    url = f"https://api.semanticscholar.org/graph/v1/paper/search?query={field}&limit=100"
    headers = {"x-api-key": api_key}
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None


paper_info = get_paper_info(paper_id, api_key)
if paper_info:
    with open('paper_ids/paper_'+field+'.json', mode='w', newline='', encoding='utf-8') as file:
        json.dump(paper_info, file, indent=4)
else:
    print("Failed to retrieve paper information.")
