import string
import random

# Bare minimum implementation of a sample url shortener
# This uses a hash map

# This actually would be a persistent DB.
# Sample implementation stores it in RAM.
dictionary = {}

def generateUrl(size=5):
	# Generating shortened url from a combination of upper,lower
	# case and digits 
	generateFrom = string.ascii_uppercase
	generateFrom += string.ascii_lowercase
	generateFrom += string.digits
	
	return ''.join(random.choice(generateFrom) for _ in range(size))

def regenerateUrl(conflict, size=5):
	pass

def shortenUrl(url):
	if url not in dictionary.keys():
		shortenedUrl = generateUrl(6)
		dictionary[url] = shortenedUrl
	return dictionary[url]

def expandUrl(shortUrl):
	for url, short in dictionary.items():
		if shortUrl == short:
			return url
	return None

if __name__=="__main__":
	# url after /
	url = "testUrlLong"
	shortUrl = shortenUrl(url)
	print shortUrl
	print expandUrl(shortUrl)
	url = "testUrlLongLong"
	shortUrl = shortenUrl(url)
	print shortUrl
	print expandUrl(shortUrl)
	url = "testUrlLongLongLong"
	shortUrl = shortenUrl(url)
	print shortUrl
	print expandUrl(shortUrl)
	print dictionary



	
