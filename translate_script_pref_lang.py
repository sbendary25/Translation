#!/usr/bin/env python
# -*- coding: utf-8 -*-
#Author: Samer Bendary
#Copyright: Zumata.com 
"""
Command-line application that translates hotel descriptions.
"""

from __future__ import print_function
from googleapiclient.discovery import build
from multiprocessing.dummy import Pool as ThreadPool
from urllib.request import urlopen
import json
import datetime

BASE_URL = 'http://data.zumata.com/hotels/'

# name of file with hotel_id and list of languages for description of said hotel. 
# line format: hotel_id | list_of_comma_delimited_languages
FILENAME = 'all-langs-zumata-id.txt'

# number of threads to run. configure relative to sockets available
NUMBER_OF_THREADS = 1000


# Returns a dict where the zumata language code is the key and the description
# in that language is the value
def getDescription(hotel_id, active_langs, supported_langs):

  lang_code = active_langs[0]

  for code in active_langs:
    if (code == 'en-US'):
      lang_code = 'en-US'
    if ((code in supported_langs) and (lang_code != 'en-US')):
      lang_code = code

  url = BASE_URL + hotel_id + '/' + lang_code + '/' + 'long.json'
  response = urlopen(url)
  data = json.loads(response.read().decode('utf-8'))

  description = data['description'].lstrip().rstrip()

  # this if statement checks for empty descriptions using lang_code
  # if the description is empty then we remove it from active_langs and
  # recursively call getDescription until a non-empty description is 
  # given or the active_lang list is empty. The latter indicates
  # that the particualr hotel has no valid description in any language
  # so that hotel_id is written out to the empty_hotel_descriptions file
  if (len(description) == 0):
    new_active_langs = active_langs
    new_active_langs.remove(lang_code)
    if (len(new_active_langs) != 0):
      description = getDescription(hotel_id, new_active_langs, supported_langs)
    if (len(new_active_langs) == 0):
      with open("empty_hotel_descriptions.txt", "a") as outfile:
        outfile.write(hotel_id + '\n')

  return {'lang_code': lang_code, 'description': description}


# Returns a list of zumata language codes. We want each hotel processed
# to have a description in each of the supported langs. Note that some zumata language codes
# map to the same google lang code, so there will be duplicate descriptions. i.e google doesn't differentiate
# between canadian french and native french, zumata does. 
def getSupportedLangs():
  supported_langs = 'en_US, es_ES, es_MX, fr_FR, fr_CA, zh_CN, zh_TW'.split(
      ', ')
  return supported_langs

# Returns a list of google language codes that will be mapped to the supported_langs
# list based on index
def getGoogleLangs():
  # this function exists becuase the zumata language codes differ from the google codes
  # that are used for the api request
  google_langs = 'en, es, es, fr, fr, zh-CN, zh-TW'.split(', ')
  return google_langs

# Returns the other zumata code that maps to the same google code i.e fr_CA to fr_FR
def getZumataComplement(zumata_lang):
  supported_langs = getSupportedLangs()
  for code in supported_langs:
    if ((code[:2] == zumata_lang[:2]) and (code != zumata_lang)):
      return code

  return zumata_lang

# This function returns a dict where the key is the zumata language code and the value is the
# complementary google language code.
def languageMapper(zumata_langs):
  zumata_to_google_lang_map = {}
  for code in zumata_langs:
    if (code[:2] == 'en'):
      zumata_to_google_lang_map[code] = 'en'
    if (code[:2] == 'es'):
      zumata_to_google_lang_map[code] = 'es'
    if (code[:2] == 'fr'):
      zumata_to_google_lang_map[code] = 'fr'
    if (code == 'zh_CN'):
      zumata_to_google_lang_map[code] = 'zh-CN'
    if (code == 'zh_TW'):
      zumata_to_google_lang_map[code] = 'zh-TW'

  return zumata_to_google_lang_map


# This function processes the list of hotel_ids and active langs
def processHotelList(filename):
  file = open(filename, 'r')
  lines = file.readlines()
  hotel_id_active_zumata_to_google_lang_map = {}

  for line in lines:
    words = line.split('|')
    hotel_id = words[0].strip()
    active_langs = words[1].lstrip().rstrip()
    active_langs = active_langs.split(',')

    hotel_id_active_zumata_to_google_lang_map[hotel_id] = active_langs

  return hotel_id_active_zumata_to_google_lang_map

# This function calls the google translate api and 
# returns the translated description 
def getTranslatedDescription(source,target,q):
  service = build('translate', 'v2', developerKey='AIzaSyAF61BG5Fgp6q3PaKcuxqkEv3Aa1_jlI6Q')
  response = service.translations().list(source=source, target=target, q=q).execute()
  return response['translations'][0]['translatedText']


# This function is the one that calls the google translate api
# and is the function that is multi threaded using pool.map 
def processHotels(hotel_id):
  active_langs = processHotelList(FILENAME)[hotel_id]

  supported_langs = getSupportedLangs()
  google_langs = getGoogleLangs()


  zumata_to_google_lang_map = languageMapper(supported_langs)

  descDict = getDescription(hotel_id, active_langs, supported_langs)

  # Translate the description into each supported_lang that isn't in
  # active_lang. translaton_dict will be converted into a JSON object which will
  # then be written to a txt file
  translation_dict = {}

  # Translated_Languages is a list of the langugages (zumata code) used for the descriptions in
  # translation_dict. Translated_Languages_Google_Codes is a list of the google code translation
  # that have been run through the api on a particular hotel. # of Translated_Languages_Google_Codes == # of api calls
  # for a particular hotel. Translated_Descriptions is a dict which uses the items in Translated_Languages
  # as a key to map to the translated description in that language
  translation_dict['Translated_Languages'] = []
  translation_dict['Translated_Languages_Google_Codes'] = []
  translation_dict['Translated_Descriptions'] = {}

  for lang in supported_langs:

    source = zumata_to_google_lang_map[descDict['lang_code']]
    target = zumata_to_google_lang_map[lang]
    q = descDict['description']

    # In order to limit redundancy, this (rather long and ugly) if statement checks the following:
    # 1. make sure we aren't translating into a language that we already have
    # 2. make sure that the source != the target
    # 3. make sure that we aren't translating a zumata code who's completment
    #    has already been translated i.e fr_FR and fr_CA both map to fr
    # 4. make sure we aren't calling the api for empty descriptions
    if (lang not in active_langs) and (source != target) and (target not in translation_dict['Translated_Languages_Google_Codes']) and (len(q) != 0):

      translated_description = getTranslatedDescription(source,target,q)
      
      # some french language descriptions represent the apostrophe with 
      # '&#39;' so this if statement replaces that
      if (zumata_to_google_lang_map[lang] == ('fr')):
        translated_description = translated_description.replace('&#39;', "'")

      translation_dict['Translated_Descriptions'][lang] = translated_description
      translation_dict['Translated_Languages'].append(lang)
      translation_dict['Translated_Languages_Google_Codes'].append(zumata_to_google_lang_map[lang])

    # The following if statement is meant to reduce redundant calls to the api. Some zumata lang codes map to the same google lang code
    # so instead of making seperate calls, this if statement will simply copy the already translated description.
    # Comp stands for complementary
    if ((target in translation_dict['Translated_Languages_Google_Codes']) and (lang not in translation_dict['Translated_Languages'])):
      comp_lang_code = getZumataComplement(lang)
      translation_dict['Translated_Descriptions'][lang] = translation_dict['Translated_Descriptions'][comp_lang_code]
      translation_dict['Translated_Languages'].append(lang)


  # Convert the dict to JSON format and write it to a txt file
  output_filename = 'txt_files/' + hotel_id + '.txt'
  if (len(translation_dict['Translated_Languages']) != 0):
    with open(output_filename, 'w', encoding = 'utf-8') as outfile:
      json.dump(translation_dict, outfile, ensure_ascii = False)



def main():

  # stopwatch
  startime = datetime.datetime.now()

  # hotel_id list
  hotel_id_list = list(processHotelList(FILENAME).keys())
  print(len(hotel_id_list))

  pool = ThreadPool(NUMBER_OF_THREADS)
  pool.map(processHotels, hotel_id_list)
  pool.close()
  pool.join()

  # end stopwatch
  endtime = datetime.datetime.now()

  # print the elapsed running time of the program
  print("total running time: " + str(endtime - startime))


if __name__ == '__main__':
  main()
