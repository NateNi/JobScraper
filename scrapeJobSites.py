from dotenv import load_dotenv
import os
from selenium import webdriver
import sqlite3
import time
from slack_sdk import WebClient
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select

def scrape_table_entries(websiteId, url, company, channel, containerXpath, titleXpath, linkXpath, titleAttribute):
    # Get the previously saved job links so that links will not be sent repeatedly
    conn = sqlite3.connect('jobs.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS jobLinks (id INTEGER PRIMARY KEY, links VARCHAR, UNIQUE(links))''')
    cursor.execute("SELECT links FROM jobLinks")
    previouslySentLinksResults = cursor.fetchall()
    previouslySentLinks = [row[0] for row in previouslySentLinksResults]
    # Get the list of new jobs from the given website (returns as a list of dictionaries with keys for job title and job links)
    jobs = getLinks(websiteId, company, url, previouslySentLinks, containerXpath, titleXpath, linkXpath, titleAttribute)
    conn.close()
    if (len(jobs)):
        send_message(jobs, company, channel)

def getLinks(websiteId, company, url, previouslySentLinks, containerXpath, titleXpath, linkXpath, titleAttribute):
    jobs = []
    driver = webdriver.Chrome()
    driver.get(url)
    # Wait for the JavaScript content to load
    time.sleep(5)
    conn = sqlite3.connect('jobs.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS jobWebsiteFilters (id INTEGER PRIMARY KEY, jobWebsiteId INT, filterXpath VARCHAR, selectValue VARCHAR, type VARCHAR)''')
    cursor.execute(f' SELECT filterXpath, selectValue, type FROM jobWebsiteFilters where jobWebsiteId = {websiteId} ')
    filterResults = cursor.fetchall()
    filters = [{'filterXpath': row[0], 'selectValue': row[1], 'type': row[2]} for row in filterResults]
    for filter in filters:
        match filter['type']:
            case 'select':
                select = Select(driver.find_element(By.XPATH, filter['filterXpath']))
                select.select_by_value(filter['selectValue'])
                time.sleep(5)
    conn.close()

    jobContainers = driver.find_elements(By.XPATH, containerXpath)
    for option in jobContainers:
        try:
            if (titleXpath):
                titleElement = option.find_element(By.XPATH, titleXpath) if titleXpath else option
                title = titleElement.get_attribute(titleAttribute) if titleAttribute else titleElement.text
            else:
                title = f'New {company} Job'
            if (linkXpath):
                link = option.find_element(By.XPATH, linkXpath).get_attribute('href') if linkXpath else option.get_attribute('href')
            else:
                link = url
            if len(previouslySentLinks) == 0 or link not in previouslySentLinks:
                jobs.append({'title': title, 'link': link})
        except NoSuchElementException:
            print("Element not found")
    driver.quit()
    return jobs

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
cursor.execute('''CREATE TABLE IF NOT EXISTS jobWebsites (id INTEGER PRIMARY KEY, url VARCHAR, company VARCHAR, channel VARCHAR, containerXpath VARCHAR, titleXpath VARCHAR, linkXpath VARCHAR, titleAttribute VARCHAR)''')
cursor.execute("SELECT * FROM jobWebsites")
jobWebsitesResults = cursor.fetchall()
websites = [{'id': row[0], 'url': row[1], 'company': row[2], 'channel': row[3], 'containerXpath': row[4], 'titleXpath': row[5], 'linkXpath': row[6], 'titleAttribute': row[7]} for row in jobWebsitesResults]
conn.close()
for website in websites:
    scrape_table_entries(website['id'], website['url'], website['company'], website['channel'], website['containerXpath'], website['titleXpath'], website['linkXpath'], website['titleAttribute'])
