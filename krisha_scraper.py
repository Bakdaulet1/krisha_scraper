import requests
from bs4 import BeautifulSoup
import pandas as pd

def scrape_data_from_urls(urls):
    """
    Scrapes data from a list of URLs and extracts specific values from <div> tags.

    Parameters:
        urls (list): List of URLs to scrape.

    Returns:
        list: A list of dictionaries containing the extracted data for each URL.
    """
    results = []

    for url in urls:
        try:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract data for specified data-name attributes
            data = {
                'url': url,
                'indust.prod_square': None,
                'indust.store_square': None,
                'indust.office_square': None
            }

            for key in ['indust.prod_square', 'indust.store_square', 'indust.office_square']:
                div = soup.find('div', {'class': 'offer__info-item', 'data-name': key})
                if div:
                    value_div = div.find('div', {'class': 'offer__advert-short-info'})
                    if value_div and value_div.text.strip():
                        data[key] = value_div.text.strip()

            results.append(data)

        except requests.RequestException as e:
            print(f"Error fetching URL '{url}': {e}")
            results.append({
                'url': url,
                'indust.prod_square': None,
                'indust.store_square': None,
                'indust.office_square': None
            })

    return results

def update_csv_with_scraped_data(csv_file):
    """
    Reads a CSV file containing URLs, scrapes additional data for each URL, and appends the new columns.

    Parameters:
        csv_file (str): Path to the CSV file containing the URLs.

    Returns:
        None
    """
    # Read the existing CSV file
    try:
        df = pd.read_csv(csv_file)
    except FileNotFoundError:
        print(f"Error: File '{csv_file}' not found.")
        return

    # Get the URLs from the CSV file (assuming the column is named 'url')
    urls = df['url'].tolist()

    # Scrape data from the URLs
    scraped_data = scrape_data_from_urls(urls)

    # Convert the scraped data to a DataFrame
    scraped_df = pd.DataFrame(scraped_data)

    # Merge the original DataFrame with the scraped data
    updated_df = pd.merge(df, scraped_df, on='url', how='left')

    # Save the updated DataFrame back to the CSV file
    updated_df.to_csv(csv_file, index=False)
    print(f"Data has been updated in {csv_file}")

# Example usage
if __name__ == "__main__":
    csv_file = "test.csv"  # Replace with your CSV file path containing URLs

    # Update the CSV with the scraped data
    update_csv_with_scraped_data(csv_file)
