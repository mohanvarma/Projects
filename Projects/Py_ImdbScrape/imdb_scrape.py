#Required modules
from bs4 import BeautifulSoup
import requests

#Optional, for UI
foundGtk = False
foundWebkit = False

try:
	import gtk
	foundGtk = True
except ImportError:
	foundGtk = False
	print "Gtk module not found"

try:
	import webkit
	foundWebkit = True
except ImportError:
	foundWebkit = False
	print "Webkit module not found"

BASE_URL = "http://www.imdb.com"

class imdbScrape:
	def __init__(self, category, string):
		# 1 for TV Show, 0 for movie
		self.category = category
		self.searchString = self.makeStr(string)
		self.soup = None
		self.url = None
		self.imgUrl = None

	
	def makeStr(self, string):
		lst = []
		searchString = ""
		if (' ' in string):
			lst = string.split(' ')
			for i in lst:
				searchString += str(i)
				searchString += "+"
			# Remove additional +
			return searchString[:-1]
		else:
			return string

	def searchImdb(self):
		# Sample searh url - "http://www.imdb.com/find?ref_=nv_sr_fn&q=12 angry&s=all" 
		searchUrl = BASE_URL+"/find?ref_=nv_sr_fn&q="+self.searchString+"&s=all"
		result = requests.get(searchUrl)

		# Parse this result with BeautifulSoup
		soup = BeautifulSoup(result.content)

		# Assuming the user gave reasonably correct search string, getting the first 
		# search result from imdb 
		firstResult = soup.find("td", "result_text")

		firstResultUrlPath = firstResult.find("a").get("href")
		self.url = BASE_URL+firstResultUrlPath
		print self.url
	
	def openTitleUrl(self):
		result = requests.get(self.url)

		soup = BeautifulSoup(result.content)
		pass

	def openImageThumbnail(self):
		if foundGtk == False or foundWebkit == False:
			return
		window = gtk.Window()
		window.set_size_request(214, 317)
		webview = webkit.WebView()
		window.add(webview)
		window.show_all()
		#webview.load_uri(self.imgUrl)
		webview.load_uri('http://ia.media-imdb.com/images/M/MV5BMTAwNDEyODU1MjheQTJeQWpwZ15BbWU2MDc3NDQwNw@@._V1_SY317_CR0,0,214,317_AL_.jpg')
		gtk.main()


if __name__=="__main__":
	print "Enter 1 for searching TV Shows, 0 for Movies"
	category = int(raw_input())
	print "Enter the title you want to search"
	title = raw_input()

	test = imdbScrape(category, title)
	test.searchImdb()
	test.openImageThumbnail()
