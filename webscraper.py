#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import numpy as np
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from config import chromedriver_path, webscraper_proxy

class WebScraper:
    """
    A web scraper using Selenium for automating web interactions.

    Args:
        url (str): The URL of the web page to be scraped.

    Attributes:
        url (str): The URL of the web page to be scraped.
        chrome_driver_path (str): Path to the Chrome driver executable.
        proxy (str): Proxy server for the browser.
        driver (webdriver.Chrome): Chrome WebDriver instance.
        content (str): Page source content of the loaded web page.
    """
    def __init__(self):
        self.chrome_driver_path = chromedriver_path
        self.proxy = webscraper_proxy
        self.driver = self.__init_driver()
        
    def __init_driver(self):
        # Determine the version of Chrome
        self.chrome_version = int(os.popen('chromium-browser --version').read().split()[1].split('.')[0])

        self.port = np.random.randint(9000,15000)
        
        # Configure Chrome options
        chrome_options = uc.ChromeOptions()
        if self.proxy:
            chrome_options.add_argument(f'--proxy-server={self.proxy}')
        chrome_options.add_argument('--remote-debugging-pipe')
        chrome_options.add_argument('--blink-settings=imagesEnabled=false')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--enable-javascript')
        chrome_options.add_argument('--disable-gpu') 
        chrome_options.add_argument(f'--port:{self.port}')       
        chrome_options.headless = True

        # # Initialize Chrome WebDriver instance
        # driver = uc.Chrome(
        #     use_subprocess=True,
        #     driver_executable_path=self.chrome_driver_path,
        #     options=chrome_options,
        #     version_main=self.chrome_version,
        #     port=38008
        # )
        # return driver

        service = Service(executable_path=self.chrome_driver_path, use_subprocess=True, port=self.port)
        driver = webdriver.Chrome(
            service=service,
            options=chrome_options,
            #version_main=self.chrome_version
        )
        return driver
    
    def url(self, url):
        self.driver.get(url)

    def current_url(self):
        return self.driver.current_url

    def content(self):
        return self.driver.page_source
    
    def snapshot(self, filename):
        """Take a screenshot of the current page and save it to a file."""
        self.driver.save_screenshot(filename + '.png')

    def write_html(self, filename):
        """Write the HTML content of the current page to a file."""
        file_to_write = open(str(filename) + '.html', "w")
        file_to_write.write(self.content)

    def fill_form_id(self, form_id, input1):
        """Fill an HTML form field by its ID with the given input value."""
        form_field = self.driver.find_element(By.ID, form_id)
        form_field.send_keys(input1)

    def click_button_xpath(self, xpath):
        """Click an HTML button using its XPath."""
        button = self.driver.find_element(By.XPATH, value=xpath)
        button.click()
    
    def click_button_id(self, form_id):
        """Click an HTML button using its ID."""
        button = self.driver.find_element(By.ID, value=form_id)
        button.click()

    def click_button_class(self, form_class):
        """Click an HTML button using its class name."""
        button = self.driver.find_element(By.CLASS_NAME, value=form_class)
        button.click()     
    
    def drop_down_class(self, form_class, value1):
        """Select an option from an HTML dropdown using its class name."""
        drop_down = Select(self.driver.find_element(By.CLASS_NAME, form_class))
        drop_down.select_by_value(value1)
    
    def maximize(self):
        """Maximize the browser window."""
        self.driver.maximize_window()

    def get_current_url(self):
        """Get the current URL of the page."""
        return self.driver.current_url

    def close(self):
        """Close the browser window."""
        self.driver.close()
    
    def quit(self):
        """Quit the browser instance."""
        self.driver.quit()

    def shutdown(self):
        self.driver.close()
        self.driver.quit()
        


# url = 'https://www.kleinanzeigen.de/s-wohnung-kaufen/c196'

# test = WebScraper(url)
