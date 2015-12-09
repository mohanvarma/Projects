# generateMap.py

# Description: This file is used to generate a visualization
# of number of BX-Users based on the country they are from.
# This data is plotted on a world Map which generates an svg file
# that can be opened in a browser.

import io
from bs4 import BeautifulSoup

#   BX-Users data-set.
#   This has information about all the 278,859 users in the data-set
#   Available info includes age and region.
#   We are parsing this data-set and extracting the country.
#   We use this data later 
#

USERS = "BX-Users.csv"

#Map colors
#COLORS = ["#F7FCFD", "#E5F5F9", "#CCECE6", "#99D8C9", "#66C2A4", "#41AE76", "#238B45", "#006D2C", "#00441B"]
COLORS = ["#FDE0DD", "#E5F5F9", "#CCECE6", "#99D8C9", "#66C2A4", "#41AE76", "#238B45", "#006D2C", "#00441B"]

#WorldMap svg template plain
MAP = "worldHigh.svg"

class generateMap:
	
	readerFreqByCountry = {}
	# init
	def __init__(self):
		self.inFileName = USERS
		# Number of users from a particular country
		# is stored here later.
		#self.readerFreqByCountry = {}

	def generateReaderFreqByCountry(self):
		inFile = open(self.inFileName, 'r')
		
		#readerFreqByCountry = {}
		# For each user in the User-Region data-set
		# Parse the country he/she is from and add it to the dictionary
		for line in inFile:
			# split the user-data based on the separator ";"
			line = line.strip().split(';')

			# get the country field
			region = line[1].strip().split(',')

			# Ignore invalid data
			if (len(region) < 3):
				continue
			
			# Clean up region field to represent a proper string 
			region = region[-1].strip()		
			region = region[:-1]

			# Data-set contains usa, but on the map, it's united states
			if region == "usa":
				region = "united states"

			# Only add it to the dictionary if it's not present already
			# otherwise, increment the count	
			if region not in generateMap.readerFreqByCountry.keys():
				generateMap.readerFreqByCountry[region] = 1
			else:
				generateMap.readerFreqByCountry[region] += 1


	# Takes an empty svg world map and outputs
	# a visualized version
	def generateSvgMap(self, inputSvg): 
		svg = open(inputSvg, 'r').read()
		# Load into Beautiful Soup
		soup = BeautifulSoup(svg, selfClosingTags=['defs','sodipodi:namedview'])

		# Find counties
		paths = soup.findAll('path')

		# Color the countries based on no. of users from that country
		matches = 0
		for p in paths:
			if p['title'] not in ["State_Lines", "separator"]:
				#print p['title']
				# Default color
				p['fill'] = COLORS[0]

				# Get all valid data and skip invalid ones
				try:
					users = generateMap.readerFreqByCountry[p['title'].lower()]
					matches += 1
					#print users
				except:
					continue

				# Categorize colors based on the no. of users
				# Darker regions mean more users
				if users > 100000:
					color_class = 8
				elif users > 50000:
					color_class = 7
				elif users > 10000:
					color_class = 6
				elif users > 5000:
					color_class = 5
				elif users > 1000:
					color_class = 4
				elif users > 500:
					color_class = 3
				elif users > 100:
					color_class = 2
				elif users > 0:
					color_class = 1
				else:
					color_class = 0

				color = COLORS[color_class]
				# Add 'fill' tag to the svg
				p['fill'] = color
        
			# No. of countries matched between map data
			# our user region data-set	
			#print matches
			
			# Generate the map 
			f = io.open("visualized.svg", 'w', encoding='utf8')
			for line in soup.prettify():
				f.write(line)
			f.close()

if __name__ == "__main__":
	map = generateMap()
	map.generateReaderFreqByCountry()
	#print map.readerFreqByCountry
	map.generateSvgMap(MAP)
