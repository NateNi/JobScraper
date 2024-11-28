from dotenv import load_dotenv
import os
from selenium import webdriver
import sqlite3
import time
from slack_sdk import WebClient
from jobLinkFunctions import getLinks

def scrape_table_entries(url, company, channel):
    driver = webdriver.Chrome()
    driver.get(url)
    # Wait for the JavaScript content to load
    time.sleep(4)
    # Get the previously saved job links so that links will not be sent repeatedly
    conn = sqlite3.connect('jobs.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS jobLinks (links VARCHAR, UNIQUE(links))''')
    cursor.execute("SELECT links FROM jobLinks")
    previouslySentLinksResults = cursor.fetchall()
    previouslySentLinks = [row[0] for row in previouslySentLinksResults]
    # Get the list of new jobs from the given website (returns as a list of dictionaries with keys for job title and job links)
    jobs = getLinks(driver, previouslySentLinks, company)
    driver.quit()
    conn.close()
    if (len(jobs)):
        send_message(jobs, company, channel)

def send_message(jobs, company, channel):

    # Create a Slack Web API client
    slack_client = WebClient(token=os.getenv('SLACK_TOKEN'))
    # Send the message to the specified channel
    links = [f"<{job['link']}|{job['title']}>" for job in jobs]
    response = slack_client.chat_postMessage(channel=channel, text= "*" + company + " jobs found: * \n\n" + "\n\n".join(links))
    # Check if the message was sent successfully
    if response["ok"]:
        print("Slack message sent successfully")
        # Add links to the jobLinks table
        conn = sqlite3.connect('jobs.db')
        cursor = conn.cursor()
        for link in (job['link'] for job in jobs):
            cursor.execute(f"INSERT OR IGNORE INTO jobLinks(links) VALUES('{link}')")
        conn.commit()
        conn.close()
    else:
        print(f"Failed to send Slack message: {response['error']}")

load_dotenv()
# Get the company names, urls, and the slack channel to post to from the jobWebsites table
conn = sqlite3.connect('jobs.db')
cursor = conn.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS jobWebsites (url VARCHAR, company VARCHAR, channel VARCHAR)''')
cursor.execute("SELECT * FROM jobWebsites")
jobWebsitesResults = cursor.fetchall()
websites = [{'url': row[0], 'company': row[1], 'channel': row[2]} for row in jobWebsitesResults]
conn.close()
for website in websites:
    scrape_table_entries(website['url'], website['company'], website['channel'])
