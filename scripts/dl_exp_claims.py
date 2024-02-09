import requests
from requests.exceptions import RequestException
import time
import concurrent.futures

from bs4 import BeautifulSoup
import pickle
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

import multiprocessing

session= requests.Session()
MAX_RETRIES = 3
DELAY_SECONDS = 5


def download_csv(url):
    response = session.get(url)
    
    if response.status_code == 200:
        return response.content
    else:
        print(f"Failed to download CSV from {url}. Status code: {response.status_code}")
        return None


def download_csvs_concurrently(urls):
    with ThreadPoolExecutor() as executor, tqdm(total=len(urls), desc="Downloading CSVs") as pbar:
        futures = {url: executor.submit(download_csv, url) for url in urls}

        for future in concurrent.futures.as_completed(futures.values()):
            pbar.update(1)

        result_dict = {url: future.result() for url, future in futures.items()}

    return result_dict


def save_dict_to_file(data_dict, file_path):
    with open(file_path, 'wb') as file:
        pickle.dump(data_dict, file)
    print(f"Dictionary saved to {file_path}")


def timing_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"{func.__name__} took {elapsed_time:.5f} seconds to execute.")
        return result
    return wrapper

@timing_decorator
def _construct_expense_urls():
    """Generates a list of URLs to MOP expense reports for different quarters."""

    def format_expense_period(s):
        year, quarter = s[:-2], s[-1]
        year= int(year)+1
        return f'{year}/{quarter}'
    
    base_url= "https://www.ourcommons.ca/ProactiveDisclosure/en/members/"
    expenses= read_json("entities/json/ExpenditureMOP.json")
    dates= set(expense.get("expenseDate", {}).get("value", "") for expense in expenses)
    dates= [format_expense_period(date) for date in dates]

    return [base_url+date for date in dates]

@timing_decorator
def _get_expense_report_hrefs(url):
    """Get the hrefs to expenditure reports for each MOP."""
    hrefs = []

    retries = 0
    while retries < MAX_RETRIES:
        try:
            response = session.get(url)
            response.raise_for_status()  # Raise an exception for 4xx and 5xx status codes

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                table = soup.find('table', class_='table table-responsive table-striped')
                rows = table.find_all('tr', class_='expenses-main-info')

                for row in rows:
                    elements = row.find_all('td', class_='text-nowrap text-right')

                    for element in elements:
                        href_element = element.find('a', class_='light-bold view-report-link')

                        if href_element:
                            hrefs.append("https://www.ourcommons.ca" + href_element['href'] + "/csv")

                return hrefs

            elif response.status_code == 500:
                # Server error, log and retry
                print(f"Server Error (500): Retrying in {DELAY_SECONDS} seconds...")
                time.sleep(DELAY_SECONDS)

            else:
                print(f"Error: Unable to fetch the page. Status code: {response.status_code}")

        except RequestException as e:
            print(f"Request Exception: {e}")
            print(f"Retrying in {DELAY_SECONDS} seconds...")
            time.sleep(DELAY_SECONDS)
            retries += 1

    print(f"Unable to fetch the page after {MAX_RETRIES} attempts.")
    return []


if __name__=="__main__":

    url_1= _construct_expense_urls()
    csv_urls= list(map(_get_expense_report_hrefs, url_1))
    csv_urls= [href for href_list in csv_urls for href in href_list]
    csv_dict = download_csvs_concurrently(csv_urls)

    # Specify the file path where you want to save the dictionary
    output_file_path = "data/csv/expense_claims.pkl"

    save_dict_to_file(csv_dict, output_file_path)