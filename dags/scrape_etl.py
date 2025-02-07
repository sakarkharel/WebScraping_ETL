from bs4 import BeautifulSoup
import requests
import pandas as pd
import csv
import numpy as np
import sqlite3


def extract():
    base_url = "https://quotes.toscrape.com/page/{}/"
    data_list = []

    for page_number in range(1, 11):
        url = base_url.format(page_number)
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()  # Raises an error for bad responses (e.g., 404, 500)
        except requests.RequestException as e:
            print(f" Error fetching {url}: {e}")
            continue  # Skip to the next page if request fails

        soup = BeautifulSoup(response.text, 'html.parser')
        quotes = soup.find_all("div", class_="quote", itemtype="http://schema.org/CreativeWork")

        for quote in quotes:
            quote_text = quote.find("span", class_="text").text.strip()
            author = quote.find("small", class_="author").text.strip()
            tags = ", ".join(tag.text.strip() for tag in quote.find_all("a", class_="tag"))  # Convert list to string

            data_list.append({
                "quote": quote_text,
                "author": author,
                "tags": tags
            })
            
    # Save data to CSV
    df = pd.DataFrame(data_list)
    df.to_csv('/opt/airflow/data/quotes.csv', encoding='utf-8', index=False)
    print("Quotes extracted and saved successfully.")


def author_bio():
    base_url = "https://quotes.toscrape.com"
    page_url = base_url + "/page/{}/"
    new_list = []
 
    for page_number in range(1, 11):  # Loop from 1 to 10
        url = page_url.format(page_number)
        page = requests.get(url)
        soup = BeautifulSoup(page.text, 'html.parser')
        quotes = soup.find_all("div", class_="quote")

        for quote in quotes:
            bio_link = quote.find("a", string="(about)")
            if bio_link:  # Ensure the link exists
                bio = bio_link["href"].strip()
                new_list.append({"default_url": base_url + bio})

    df = pd.DataFrame(new_list)
    df.to_csv('/opt/airflow/data/linkforauthors.csv', encoding='utf-8', index=False)
    return new_list  


def go_through():
    df = pd.read_csv('/opt/airflow/data/linkforauthors.csv')
    new_data = []

    for _, row in df.iterrows():  # Iterate over DataFrame rows
        link = row['default_url']
        page = requests.get(link)
        soup = BeautifulSoup(page.text, 'html.parser')

        # Extract birth date and birth location
        birth_date_tag = soup.find("span", class_="author-born-date")
        birth_location_tag = soup.find("span", class_ = "author-born-location")
        birth_date = birth_date_tag.text.strip()
        birth_location = birth_location_tag.text.strip()
        new_data.append({"birth_date" : birth_date, "birth_location": birth_location})
        df1 = pd.DataFrame(new_data)
        df1.to_csv('/opt/airflow/data/complete.csv', encoding = 'utf-8', index = False)

def join():
    df = pd.read_csv('/opt/airflow/data/quotes.csv')
    df1 = pd.read_csv('/opt/airflow/data/complete.csv')
    new_df = pd.merge(df, df1, left_index=True, right_index=True, how="outer")
    new_df.to_csv('/opt/airflow/data/bio.csv', encoding = 'utf-8', index = False)
    
def clean():
    df3 = pd.read_csv('/opt/airflow/data/bio.csv')
    df3['tags'] = df3['tags'].str.replace(r'\[|\]', '', regex=True)
    df3['birth_location'] = df3['birth_location'].str.replace(r'\bin\b', '', regex = True)
    df3['tags'] = df3['tags'].str.replace(r'\'', '', regex = True) 
    df3['quote'] = df3['quote'].str.replace(r'[\u201C\u201D]', "", regex=True)
    df3['birth_date'] = pd.to_datetime(df3['birth_date'], format='%B %d, %Y').dt.strftime('%Y-%m-%d')
    df3.to_csv('/opt/airflow/data/transformed.csv', encoding = 'utf-8', index= False)
   

if __name__ == "__main__":
    extracted_data = extract()
    author_bio()
    go_through()
    join()
    clean()


                



