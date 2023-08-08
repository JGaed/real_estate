#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import misc
import time
import pandas as pd
from webscraper import WebScraper
import dateutil.parser as dparser
import pgeocode
import datetime
import numpy as np

class Kleinanzeigen:
    SEARCH_TEMPLATE_URL = 'https://www.kleinanzeigen.de/s-wohnung-kaufen/c196'
    OFFER_TEMPLATE_URL = 'https://www.kleinanzeigen.de/s-anzeige/{index}'
    OFFERS_PER_PAGE = 25

    @classmethod
    def create_df(cls, postalcode, radius=None, pages=None, end_index=None):
        """
        Args:
            postalcode (str): The postal code to search for properties.
            radius (int): The search radius in kilometers from the given postal code.
            pages (list[int]): List of page numbers to scrape.
            end_index (int): The end index to stop scraping.
            
        Returns:
            pd.DataFrame: DataFrame containing the scraped property listings.
        """
        offers = cls.SearchPage(postalcode, radius=radius, pages=pages, end_index=end_index)
        df = pd.concat([cls.OfferPage(i).to_df() for i in offers.offers_indices], ignore_index=True)
        return df

    @classmethod
    def to_mysql(cls, postalcode, radius=None, pages=None, end_index=None, max_number=None):
        """
        Args:
            postalcode (str): The postal code to search for properties.
            radius (int): The search radius in kilometers from the given postal code.
            pages (list[int]): List of page numbers to scrape.
            end_index (int): The end index to stop scraping.
            max_number (int): Maximum number of entries to write.
            
        Returns:
            None
        """
        columns = ('postalcode', 'state', 'state_code', 'place', 'price', 'size', 'rooms', 'floor', 'date', 'id', 'timestamp')
        offers = cls.SearchPage(postalcode, radius, pages=pages, end_index=end_index, max_number=max_number)
        offers_in_database = [int(x[0]) for x in misc.MySQL.get_table('id', 'Kleinanzeigen')]
        new_offers = [x for x in offers.offers_indices if x not in offers_in_database]
        print('[PYTHON][KLEINANZ][TO_MYSQL][PROGRESS] Scraping offers: {}'.format(len(new_offers)))
        print(new_offers)
        values = []
        for i in new_offers:
            try:
                offer = cls.OfferPage(i)
                values_i = (
                    offer.postalcode, offer.state, offer.state_code, offer.place, offer.price, offer.size,
                    offer.rooms, offer.floor, offer.date.date(), offer.index, datetime.datetime.now()
                )
                values.append(values_i)
            except Exception as e:
                print('[PYTHON][KLEINANZ][TO_MYSQL][ERROR]', i, e)
        misc.MySQL.write_list('Kleinanzeigen', columns, values)

    class SearchPage():
        """
        Represents the search page of Kleinanzeigen.
        
        Args:
            postalcode (str): The postal code to search for properties.
            radius (int): The search radius in kilometers from the given postal code.
            pages (list[int]): List of page numbers to scrape.
            end_index (int): The end index to stop scraping.
            max_number (int): Maximum number of entries to scrape.
        
        Attributes:
            end_index (int): The end index to stop scraping.
            postalcode (str): The postal code to search for properties.
            radius (int): The search radius in kilometers from the given postal code.
            url_search_page (str): The URL for the search page based on postal code and radius.
            pages (list[int]): List of page numbers to scrape.
            max_number (int): Maximum number of entries to scrape.
            offers_indices (list[int]): List of indices of scraped property offers.
        """
        def __init__(self, postalcode, radius=None, pages=None, end_index=None, max_number=None):
            self.end_index = end_index
            self.postalcode = postalcode
            self.radius = radius
            self.url_search_page = self.__get_index_page_url()
            self.pages = pages
            self.max_number = max_number
            self.offers_indices = []
            self.__init_search_pages()

        def __get_index_page_url(self):
            page = WebScraper(Kleinanzeigen.SEARCH_TEMPLATE_URL)
            time.sleep(1)
            try:
                page.click_button_id("gdpr-banner-accept")
                time.sleep(3)
            except:
                print('[PYTHON][KLEINANZ][SEARCH_PAGE][PROGRESS] No Popup')
            try:
                page.click_button_xpath('//*[@id="site-signin"]/div/div/a')
            except:
                print('[PYTHON][KLEINANZ][SEARCH_PAGE][PROGRESS] No Popup')
            page.fill_form_id("site-search-area", self.postalcode)
            page.click_button_xpath('//*[@id="site-search-submit"]')
            url = page.get_current_url()
            url = url.replace('//', '<<<<')
            url = url.split('/')
            url.insert(-1, "seite:{page}")
            url = '/'.join(url).replace('<<<<', '//')
            page.quit()
            return url

        def __init_search_pages(self):
            self.offers_indices = []
            i = 0
            page_i = 0
            while True:
                # Determine the current page number to scrape
                if self.pages:
                    page_i = self.pages[i]
                    i += 1
                else:
                    page_i += 1

                # Construct the URL for the current page
                if self.radius:
                    url_i = (self.url_search_page + "r{radius}").format(page=page_i, radius=self.radius)
                else:
                    url_i = self.url_search_page.format(page=page_i)

                print(url_i)
                page = WebScraper(url_i)
                offer_indices_i = self.__get_offer_index(page.content.split('\n'))
                max_page = self.__get_max_page(page.content)
                print('[PYTHON][KLEINANZ][SEARCH_PAGE][PROGRESS] Current page:', page_i)
                print('[PYTHON][KLEINANZ][SEARCH_PAGE][PROGRESS] Current max page:', max_page)

                # Check if end_index condition is met
                if self.end_index:
                    if type(self.end_index) == int:
                        self.end_index = [self.end_index]
                    if any(x in self.end_index for x in offer_indices_i):
                        self.offers_indices += offer_indices_i[:offer_indices_i.index(self.end_index)]
                        break

                self.offers_indices += offer_indices_i

                # Check if max_number condition is met
                if self.max_number and len(self.offers_indices) >= self.max_number:
                    self.offers_indices = self.offers_indices[:self.max_number]
                    break

                # Check if the end of pages or maximum page count is reached
                if page_i == max_page or (self.pages and i == len(self.pages)):
                    break

                page.close()

            page.quit()

        # def __get_max_page(self, content):
        #     pages_lines_start =  misc.get_lines(content.split('\n'), "srchrslt-pagination")[1][0]
        #     pages_lines_end =  [x for x in misc.get_lines(content.split('\n'), "</div>")[1] if x > pages_lines_start][1]
        #     if len(misc.get_lines(content.split('\n')[pages_lines_start:pages_lines_end], "seite:")[0]) > 0:
        #         max_page = misc.get_numbers(misc.get_lines(content.split('\n')[pages_lines_start:pages_lines_end], "seite:")[0][-1])[1]
        #     else:
        #         max_page = 1
        #     return max_page
        
        def __get_max_page(self, content):
            total_offers = misc.get_floats(misc.get_lines(content.split('\n'), "breadcrump-summary")[0][0])[-2]
            max_page = np.rint(total_offers/Kleinanzeigen.OFFERS_PER_PAGE)
            return int(max_page)
        
        def __get_offer_index(self, content, filter_out_top=True):
            self.offer_lines = misc.get_lines(content, 'data-adid=')[1]
            if filter_out_top:
                top_lines = misc.get_lines(content, 'badge-topad is-topad')[1]
                self.offer_lines = [x for x in self.offer_lines if x-1 not in top_lines]
            
            return [int(misc.get_numbers(content[x].split()[2])[0]) for x in self.offer_lines]   

    class OfferPage:
        """
        Represents the offer page of Kleinanzeigen.

        Args:
            offer_index (int): The index of the offer to retrieve details for.

        Attributes:
            index (int): The index of the offer.
            url (str): The URL of the offer page.
            content (list[str]): The content of the offer page.
            title (str): The title of the offer.
            date (datetime): The date of the offer.
            price (float): The price of the offer.
            postalcode (int): The postal code of the property.
            state (str): The state of the property location.
            state_code (str): The state code of the property location.
            place (str): The city or place of the property.
            size (int): The size of the property in square meters.
            rooms (float): The number of rooms in the property.
            floor (int): The floor of the property.
            build_year (int): The year the property was built.
        """
        def __init__(self, offer_index):
            # Initialize instance variables
            self.index = offer_index
            self.url = Kleinanzeigen.OFFER_TEMPLATE_URL.format(index=self.index)
            self.content = self.__get_offer_content()
            self.__set_details_NULL()  # Set initial details to None
            self.__get_title()
            self.__get_date()
            self.__get_price()
            self.__get_postalcode()
            self.__get_city()
            self.__get_all_details()
            self.__get_filtered_details()
            self.__print()

        def __set_details_NULL(self):
            """Set initial values of details attributes to None."""
            self.size = None
            self.rooms = None
            self.floor = None
            self.build_year = None

        def __get_offer_content(self):
            """Get the content of the offer page using a WebScraper instance."""
            page = WebScraper(self.url)
            content = page.content.split('\n')
            page.quit()
            return content

        def __get_title(self):
            """Extract and store the title of the offer."""
            self.title = misc.get_lines(self.content, '<title>')[0][0].split(sep='>')[1].split(sep='<')[0].split('|')[0]

        def __get_date(self):
            """Extract and store the date of the offer."""
            self.date = dparser.parse(misc.get_lines(self.content, 'icon icon-small icon-calendar-gray-simple')[0][0], fuzzy=True, dayfirst=True)
            self.day = self.date.day
            self.month = self.date.month
            self.year = self.date.year

        def __get_price(self):
            """Extract and store the price of the offer."""
            num1 = misc.get_numbers(misc.get_lines(self.content, 'adPrice:')[0][0])[0]
            num2 = misc.get_numbers(misc.get_lines(self.content, 'adPrice:')[0][0])[1]
            self.price = float(int(num1) + (int(num2) / 100))

        def __get_postalcode(self):
            """Extract and store the postal code of the property."""
            adress_index = misc.get_lines(self.content, 'initMap')[1][0] + 1
            self.adress_line = self.content[adress_index]
            try:
                number = (re.findall(r"\D(\d{5})\D", " " + self.adress_line + " "))[0]
                self.postalcode = int(number)
            except:
                print('[PYTHON][KLEINANZ][OFFER_PAGE][POSTALCODE][WARNING] Not type(int): {}'.format(number))

        def __get_city(self):
            """Query and store the state, state code, and city of the property location."""
            data = pgeocode.Nominatim('de').query_postal_code(str(self.postalcode))
            self.state = data['state_name']
            self.place = data['place_name']
            self.state_code = data['state_code']

        def __get_all_details(self):
            """Extract and store all details of the property."""
            details = [x.split(sep='<')[0].split()[0] for x in misc.get_lines(self.content, '<span class="addetailslist--detail--value">')[0]]
            value_lines = [x + 1 for x in misc.get_lines(self.content, '<span class="addetailslist--detail--value">')[1]]
            values = [' '.join(self.content[x].split(sep='<')[0].split()) for x in value_lines]
            self.details = dict(zip(details, values))

        def __get_filtered_details(self):
            """Extract and store specific details of the property with data type conversion."""
            keys = list(self.details.keys())

            if 'Wohnfläche' in keys:
                try:
                    self.size = int(misc.get_numbers(self.details['Wohnfläche'])[0])
                except:
                    print('[PYTHON][KLEINANZ][OFFER_PAGE][ROOMS][WARNING] Not type(int): {}'.format(misc.get_numbers(self.details['Wohnfläche'])[0]))
            if 'Zimmer' in keys:
                try:
                    self.rooms = float(self.details['Zimmer'].replace(',', '.'))
                except:
                    print('[PYTHON][KLEINANZ][OFFER_PAGE][ROOMS][WARNING] Not type(float): {}'.format(self.details['Zimmer']))
            if 'Etage' in keys:
                try:
                    self.floor = int(self.details['Etage'])
                except:
                    print('[PYTHON][KLEINANZ][OFFER_PAGE][FLOOR][WARNING] Not type(int): {}'.format(self.details['Etage']))
            if 'Baujahr' in keys:
                try:
                    self.build_year = int(self.details['Baujahr'])
                except:
                    print('[PYTHON][KLEINANZ][OFFER_PAGE][BUILD_YEAR][WARNING] Not type(int): {}'.format(self.details['Baujahr']))

        def __print(self):
            """Print a completion message with the offer index."""
            print('[PYTHON][KLEINANZ][OFFER_PAGE][COMPLETE] Index: {}'.format(self.index))

        def to_df(self):
            """
            Convert offer details to a DataFrame.

            Returns:
                pd.DataFrame: A DataFrame containing offer details.
            """
            return pd.DataFrame([{
                'title': self.title,
                'postalcode': self.postalcode,
                'state': self.state,
                'state_code': self.state_code,
                'place': self.place,
                'price': self.price,
                'size': self.size,
                'rooms': self.rooms,
                'floor': self.floor,
                'year': self.year,
                'month': self.month,
                'day': self.day,
                'kleinanz-index': self.index,
            }])

# Example usage:
if __name__ == "__main__":
    postalcode = "22303"
    radius = 20
    pages = ([1,2,3,5])
    end_index = 50
    max_number = 100

    # df = Kleinanzeigen.create_df(postalcode, radius=radius, pages=pages, end_index=end_index)
    Kleinanzeigen.to_mysql(postalcode, radius=radius, pages=pages, end_index=end_index, max_number=max_number)
