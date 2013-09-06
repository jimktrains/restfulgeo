import re
import psycopg2
import jellyfish
from itertools import *
from suffix import *

# Taken from
# http://docs.python.org/2/library/itertools.html#recipes
def powerset(iterable):
   "powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
   s = list(iterable)
   return chain.from_iterable(combinations(s, r) for r in range(len(s)+1))

def zipcode_to_city_list(zipcode, conn):
    sql = "SELECT UPPER(cousub.name), state.stusps \
           FROM tl_2013_42_cousub AS cousub \
               INNER JOIN tl_2013_42_addfeat AS addr ON  ST_Contains(cousub.geom, addr.geom) AND ( addr.zipr = %(zipcode)s OR addr.zipl = %(zipcode)s) \
               INNER JOIN tl_2013_us_state as state ON state.statefp = cousub.statefp";
    
    cur = conn.cursor()
    cur.execute(sql, {'zipcode':zipcode});
    
    zips = [(x[0], x[1]) for x in cur]

    # This list of lambdas are a collection of changes that
    # could be made to an address. They should accept and return
    # in all caps with no punctuation
    # This is only a small portion of the possible modifications
    # but I was only targeting common ones.
    # The list is of Lambdata, not tuples because
    # I originally had modifications that wern't simple subsitutions
    base_city_modifications = [
      lambda t : (t[0].replace('SAINT', 'ST'), t[1]),
      lambda t : (t[0].replace('CENTER', 'CTR'), t[1]),
      lambda t : (t[0].replace('JUNCTION', 'JCT'), t[1]),
      lambda t : (t[0].replace('EAST', 'E'), t[1]),
      lambda t : (t[0].replace('WEST', 'W'), t[1]),
      lambda t : (t[0].replace('NORTH', 'N'), t[1]),
      lambda t : (t[0].replace('SOUTH', 'S'), t[1])
    ]
    # Now lets take all combinations of our modifications
    # and apply them to the base city and append all the possible
    # changes to our list of cities
    city_modifications = powerset(base_city_modifications)
    mod_city_names = []
    for city_state in zips:
       city_state = (re.sub(r'\s+', ' ', re.sub(r'[^0-9a-zA-Z]',' ', city_state[0])), city_state[1])
       for mods in city_modifications:
         c = city_state
         for mod in mods:
            c = mod(c)
         mod_city_names.append(c)
    return set(sorted(zips + mod_city_names, key=lambda x : len(x[0]), reverse=True))

def string_to_address(raw_address, conn):
    original_raw_address = raw_address
    address = {
      'street_number': None, # This isn't yet parsed out, but it's on the list
      'street_number_post': None, # 123 1/2 Main St
      'street_pre_dir': None, # _N_ Main St
      'street': None, # PO Boxes will be stuck into this field, fwiw
                     # street are currently kept in this field
      'street_type': None, # e.g. Ave St Rd
      'city': None,
      'state': None,
      'zipcode': None,
      'zipcode+4': None,
    }


    # Commas and extra spaces are annoying
    # burn them!
    raw_address = re.sub(r'\s+', ' ', raw_address)
    raw_address = raw_address.replace(',', '')

    # All US Zipcodes are 5 digits with an optional +4 section, a hyphen followed by 4 digits
    matches = re.finditer(r'((\d{5})(-\d{4})?)', raw_address)
    good_zip_found = False
    for canidate_zipcode in matches:
       # If the canidate zipcode is closer to the start
       # of the raw address it's probably not a zipcode
       if canidate_zipcode.start() < 10:
          continue
       # Since we have a canidate not near the start, lets
       # go ahead and use it. Note that this means we're using
       # the last canidate...
       good_zip_found = canidate_zipcode
       # ...unless it's a zip+4 code, then we're pretty sure
       # we're dealing with a zipcode, so we'll just use that,
       # really
       if canidate_zipcode.group(3) is not None:
          break
    if not good_zip_found:
      raise Exception("No canidate zipcode found.\n\tRaw: %s" % raw_address)

    zipcode_match = canidate_zipcode

    # Store the full Zip+4
    address['zipcode'] = zip5 = zipcode_match.group(2)
    address['zipcode+4'] = zip4 = zipcode_match.group(3)

    # This is a super naive way of doing this, but
    # there were quite a few misspellings in the data
    # We create a sliding window, requiring the character
    # befor the window to be not alphanumeric (assuming
    # a space doesn't work because some people don't use spaces:-\)
    # the length of the canidate city and slide it from index 5
    # to the end. (Why 5? The city won't be that early
    # in the string.) We then take the contents of the
    # sliding window and calculator the Jaro-Winkler
    # Distance (https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance)
    # of it and the canidate.  We then take the best
    # window, and if its score is greater greater than
    # 90% (Why 90%? I felt like it?) we'll use that as
    # the city, otherwise we discard and move on.
    raw_address_upper = raw_address.upper()
    cities = zipcode_to_city_list(zip5, conn)
    best_city_start_index = None
    best_city_name = None
    best_score = 0
    for (canidate_city, canidate_state) in cities:
       for i in range(5, zipcode_match.start()):
         if not raw_address_upper[i-1].isalnum():
            score = jellyfish.jaro_distance(canidate_city, raw_address_upper[i:i+len(canidate_city)])
            if score > best_score:
               best_city_start_index = i
               best_score = score
               best_city_name = canidate_city
    if best_score < 0.9:
       best_city_start_index = None
    if best_city_start_index:
       address['state'] = canidate_state
       address['city'] = raw_address[best_city_start_index:best_city_start_index+len(best_city_name)]
       address['street'] = raw_address[:best_city_start_index-1] #rm the space

    # OK, I really don't know what's going on
    if address['street'] is None or address['city'] is None:
       raise Exception("No cities in the found zipcode match the raw address string.\n\tRaw: %s\n\tCanidates: %s" % (raw_address_upper, cities))

    # It should be PO Box, not anything else
    # Pub 28 sec 2.28
    # http://pe.usps.com/text/pub28/28c2_037.htm
    address['street'] = address['street'].replace('P.O.', 'PO').\
                                replace('P O', 'PO').\
                                replace('P. O.', 'PO').\
                                replace('BOX', 'Box')

    # Some abbreviations from
    # Publication 28 Appendix G: Business Word Abbreviations
    # http://pe.usps.com/text/pub28/28apg.htm
    terms_to_switch = {
     "Building": "BLDG",
     "Station": "STA",
     "Plaza": "PLZ",
     "Memorial": "MEML",
     "Office": "OFC",
     "North": "N",
     "South": "S",
     "West": "W",
     "East": "E",
     "Avenue": "AVE",
     "Street": "ST",
     "Road": "RD",
     "Circle": "CIR",
     "Center": "CTR",
     "House": "HSE",
     "Suite": "STE",
     "Square": "SQR",
     "Capitol": "CPTOL",
     "Capital": "CPTAL",
     "Senator": "SEN",
     "Representative": "REP",
     "Leader": "LDR",
    }
    address['street'] = address['street'].replace('.', '')

    # Check if we fixed or did anything?
    # If not a last ditch effort is to replace 
    # what we can with abbreviations. We search
    # only for full words, not prefixes or suffixes.
    #
    # i.e.: Abbr. ALL THAT THINGS!
    for first_line in address['street'].split("\n"):
     if len(first_line) > 40 or len(first_line.split(' ')) > 8:
        for term in terms_to_switch:
           address['street'] = re.sub(r"(\b)%s(\b)" % term, r"\1%s\2" % terms_to_switch[term], address['street'])

    if -1 == address['street'].find("PO Box"): # No need to go any further if it's a PO Box
        suffix_canidate = "|".join(street_suffix)
        dir_canidate = "|".join(("N","W","S","E"))
        
        canidate = re.match(r"(?P<street_number>\d+)\s+((?P<other>.*)\s+)?((?P<predir>" + dir_canidate + ")\s+)?(?P<street>.*)\s+?(?P<street_type>"+suffix_canidate+")?", address['street'], re.I)
        
        if canidate:
            canidate = canidate.groupdict()
            address['street_number'] = canidate['street_number']
            address['street'] = canidate['street']
            address['street_pre_dir'] = canidate['predir']
            address['street_type'] = canidate['street_type']
            address['street_number_post'] = canidate['other']
    return address
