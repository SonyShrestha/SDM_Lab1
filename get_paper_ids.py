import argparse
import json
import requests
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)  # Set log level to INFO

# Create logger object
logger = logging.getLogger()

# Function to retrieve paper ids of given query type
def get_paper_info(api_key, field):
    url = f"https://api.semanticscholar.org/graph/v1/paper/search?query={field}&limit=100&offset=0"
    headers = {"x-api-key": api_key}
    logger.info('Making API Call to fetch paper ids for field %s', field)  
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        logger.error("Error: %s", response.status_code)  
        return None

def main():
    # Create argument parser
    parser = argparse.ArgumentParser(description="Retrieve paper information from Semantic Scholar API")

    # Add arguments
    parser.add_argument("--field", required=True, help="Comma separated fields to search for papers")
    parser.add_argument("--api_key", required=True, help="API key for accessing Semantic Scholar API")

    # Parse arguments
    args = parser.parse_args()
    fields = args.field.split(',')

    # Retrieve paper information
    for field in fields:
        paper_info = get_paper_info(args.api_key, field)
        if paper_info:
            with open(f'paper_ids/paper_{field}.json', mode='w', encoding='utf-8') as file:
                json.dump(paper_info, file, indent=4)
        else:
            print("Failed to retrieve paper information.")

if __name__ == "__main__":
    main()
