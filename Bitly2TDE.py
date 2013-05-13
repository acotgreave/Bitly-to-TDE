# -----------------------------------------------------------------------
# Andy's Bitly>TDE reader
# -----------------------------------------------------------------------
import sys
import csv
import time
import datetime
import locale
import array
import re
import ConfigParser
import bitly_api
import json
import os
from config import config
import dataextract as tde # Tableau Data Engine API


def norm(s):
    if type(s) == unicode:
        return s.encode('utf-8').strip()
    else:
        return s

# create the connection and get all my bundles
c = bitly_api.Connection(access_token=config['ACCESS_TOKEN'])

#j = c.bundle_bundles_by_user(config['u'])
j = c.user_bundle_history()

myBundles = j['bundles']

# open the extract
try:  # Just for testing purposes and re-running
    tdefile = tde.Extract('bitlyDetails.tde') #in CWD
except: 
    os.remove('bitlyDetails.tde')
    os.remove('DataExtract.log')
    tdefile = tde.Extract('bitlyDetails.tde')

print 'tde ok'

# make the extract and define the columns
tableDef = tde.TableDefinition()
tableDef.addColumn("bundle_link", tde.Type.CHAR_STRING)   
tableDef.addColumn("bundle_title", tde.Type.CHAR_STRING) 
tableDef.addColumn("bundle_owner", tde.Type.CHAR_STRING) 
tableDef.addColumn("bundle_created", tde.Type.DOUBLE) 
tableDef.addColumn("bundle_description", tde.Type.CHAR_STRING) 
tableDef.addColumn("bundle_last_mod", tde.Type.DOUBLE) 
tableDef.addColumn("bundle_IsPriv", tde.Type.BOOLEAN) 
tableDef.addColumn("link_title", tde.Type.CHAR_STRING) 
tableDef.addColumn("link_aggregate_link", tde.Type.CHAR_STRING) 
tableDef.addColumn("link_long_url", tde.Type.CHAR_STRING) 
tableDef.addColumn("link_link", tde.Type.CHAR_STRING) 
tableDef.addColumn("clickCountry", tde.Type.CHAR_STRING) 
tableDef.addColumn("clickCount", tde.Type.DOUBLE)

table = tdefile.addTable('Extract', tableDef)
print 'table ok'

for bundle in myBundles:
    # get all the links
    bundle_link = bundle['bundle_link']
    bundle_owner= bundle['bundle_owner']
    bundle_created = bundle['created_ts']
    bundle_description = bundle['description']
    bundle_last_mod = bundle['last_modified_ts']
    bundle_IsPriv = bundle['private']
    bundle_title = bundle['title']
    #print 'Created: ' + str(bundle_created) + '; desc: ' + bundle_description + '; link: ' + bundle_link
    bundleLinks = c.bundle_contents(bundle_link)
    myBundleLinks = bundleLinks['bundle']
    print bundle_link
    # now get all the links inside the bundle
    for link in myBundleLinks['links']:
        link_title = link['title']
        link_aggregate_link = link['aggregate_link']
        link_long_url = link['long_url']
        link_link = link['link']
        # now get total clicks
        myClicks = c.link_clicks(link_link)
        print link_title
        # and the country clicks
        myCountryClicks = c.link_countries(link_link)
        if not myCountryClicks:
            print 'no clicks'
            newrow = tde.Row(tableDef)
            newrow.setCharString(0, norm(bundle_link))
            newrow.setCharString(1, norm(bundle_title))
            newrow.setCharString(2, norm(bundle_owner))
            newrow.setDouble(3, norm(bundle_created))
            newrow.setCharString(4, norm(bundle_description))
            newrow.setDouble(5, norm(bundle_last_mod))
            newrow.setBoolean(6, norm(bundle_IsPriv))
            newrow.setCharString(7, norm(link_title))
            newrow.setCharString(8, norm(link_aggregate_link))
            newrow.setCharString(9, norm(link_long_url))
            newrow.setCharString(10, norm(link_link))
            newrow.setCharString(11, norm('N/A'))
            newrow.setDouble(12, norm(0))
            table.insert(newrow)
        else:
            print 'clicks'
            for country in myCountryClicks:
                clickCountry = country['country']
                clickCount = country['clicks']
                try:
                    newrow = tde.Row(tableDef)
                    newrow.setCharString(0, norm(bundle_link))
                    newrow.setCharString(1, norm(bundle_title))
                    newrow.setCharString(2, norm(bundle_owner))
                    newrow.setDouble(3, norm(bundle_created))
                    newrow.setCharString(4, norm(bundle_description))
                    newrow.setDouble(5, norm(bundle_last_mod))
                    newrow.setBoolean(6, norm(bundle_IsPriv))
                    newrow.setCharString(7, norm(link_title))
                    newrow.setCharString(8, norm(link_aggregate_link))
                    newrow.setCharString(9, norm(link_long_url))
                    newrow.setCharString(10, norm(link_link))
                    newrow.setCharString(11, norm(clickCountry))
                    newrow.setDouble(12, norm(clickCount))
                    table.insert(newrow)
                    print 'row ok'
                except ValueError, message:
                    print link_link
                    print link_title
                    print clickCountry
                    print clickCount
                    print ValueError
                    print message
                    raise ValueError
tdefile.close()

# build into some tables, or maybe just one(using ShortUrl as uniqueid)
    #1 - Links: ShortURL, LongURL, Title ,Clicks
    #2 - Links by day: ShortURL, Date, Clicks
    #3 - Link clicks by Country: ShortURL, Country, Clicks
    #3 - Bundle contents: bundle_link, description, title, link [the ShortURL identifier]
# as a flat table, it would be like this:
# ShortURL, LongURL, Title, Clicks [link total], Date, Clicks [clicks by day], bundle_link, description [bundle], title [bundle]
