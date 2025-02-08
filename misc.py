#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
from config import debug
import pandas as pd
import pickle
import bz2
from bs4 import BeautifulSoup

def clean_html(content):
    # Decode HTML entities like &lt; to < and &gt; to >
    content = content.replace('&lt;', '<').replace('&gt;', '>')

    # Use BeautifulSoup again to parse the content and extract text
    content_soup = BeautifulSoup(content, 'html.parser')

    # Get only the text and remove any HTML leftovers
    text_content = content_soup.get_text(separator=' ', strip=True)
    return text_content

def get_numbers(string_input):
    """
    Extracts and returns all numerical values from a string.

    Args:
        string_input (str): Input string containing numerical values.

    Returns:
        list: List of extracted numerical values.
    """
    return re.findall(r'\b\d+\b', string_input)

def get_lines(list_input, string_input):
    """
    Returns lines containing a specific string within a list and their corresponding line numbers.

    Args:
        list_input (list): Input list of strings.
        string_input (str): String to search for within the list.

    Returns:
        tuple: Tuple containing two lists: lines containing the specified string and their line numbers.
    """
    lines = [s for idx, s in enumerate(list_input) if string_input in s]
    lines_number = [idx for idx, s in enumerate(list_input) if string_input in s]
    return lines, lines_number

def chunkIt(seq, num):
    """
    Divides a sequence into approximately equal-sized chunks.

    Args:
        seq (list): Input sequence to be divided.
        num (int): Number of chunks to create.

    Returns:
        list: List of divided chunks.
    """
    avg = len(seq) / float(num)
    out = []
    last = 0.0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg

    return out

# Yield successive n-sized
# chunks from l.
def divide_chunks(list1, chunk_size):
    
    # looping till length l
    return [list1[i:i + chunk_size] for i in range(0, len(list1), chunk_size)] 

def get_floats(string):
    numbers = []
    string = string.split()
    string = [x.replace(',','').replace('.','') for x in string]
    for x in string:
        try:
            numbers.append(float(x))
        except:
            None
        
    return numbers

def dprint(string):
    if debug >0:
        print(string)

# Load any compressed pickle file
def decompress_pickle(file):
    data = bz2.BZ2File(file, 'rb')
    data = pickle.load(data)
    return data
    
def compressed_pickle(title, data):
    with bz2.BZ2File(title + '.pbz2', 'w') as f:
        pickle.dump(data, f)

# loads and returns a pickled objects
def loosen(file):
    pikd = open(file, 'rb')
    data = pickle.load(pikd, encoding='latin1')
    pikd.close()
    return data

# Saves the "data" with the "title" and adds the .pickle
def full_pickle(title, data):
    pikd = open(title + '.pickle', 'wb')
    pickle.dump(data, pikd)
    pikd.close()  
   