
import scrapy
from scrapy import http
from scrapy import Spider
import csv

def extract(selector, allowMiss=True, allowEmpty=True):
    '''Call extract() on the argument, strip out all whitespace, and return the first element that
    actually contains some data. Basically a replacement for x.extract()[0].strip() but a bit better
    when the text nodes are separated by a comment or something.'''
    if len(selector) == 0:
        if allowMiss:
            return ""
        raise KeyError("Not found: " + str(selector))
    text = [x.strip() for x in selector.extract()]
    for t in text:
        if t:
            return t
    if not allowEmpty:
        raise KeyError("No text in " + str(selector))
    return ""


class StackSpider(Spider):

    # Specify the name of the crawler, the accepted domains, and the first URL the scrawler will request.

    name = "erowid"
    allowed_domains = ["erowid.org"]
    start_urls = [
        "https://erowid.org/experiences/exp_list.shtml",
    ]

    def parse(self, response):

        # I found a method here of going through every index URL on Erowid. There are 764 substances and 54 categories of experiences.
        # I figured this out manaully, just messing around with the site structure. I add each request to a list and pass that along.

        requests = []

        # This will get a few popular and well-known substances.
        relevant = ['1','2','3','6','11','13','27','31','39','47']
        for drug in relevant:
            j = 1
            while j <= 54:
                url = 'https://erowid.org/experiences/exp.cgi?S='+drug+'&C='+str(j)+'&ShowViews=0&Cellar=0&Start=0&Max=5000'
                requests.append(scrapy.http.Request(url, callback=self.index_Page))
                j += 1
                
        '''
        This will get all data. Takes about two days to run.
        i = 1
        while i <= 764:
            j = 1
            while j <= 54:
                url = 'https://erowid.org/experiences/exp.cgi?S='+str(i)+'&C='+str(j)+'&ShowViews=0&Cellar=0&Start=0&Max=5000'
                requests.append(scrapy.http.Request(url, callback=self.index_Page))
                j += 1
            i += 1
        '''
        for request in requests:
            yield request


    def index_Page(self, response):

        # We reach the trip index here. Collect the href of the trip journal and request the diary page.

        requests = []
        baseUrl = 'https://erowid.org/experiences/'

        category = extract(response.xpath('/html/body/center[2]/table/tr[2]/td[1]/font/b/a/font/u/text()'))
        meta = {'category': category}

        rows = response.xpath('//*[@id="results-form"]/table/tr')
        for row in rows:
                href = extract(row.xpath('td[2]/a/@href'))
                if href:
                    request = baseUrl + href
                    requests.append(scrapy.http.Request(request, callback =self.data_Extraction, meta = meta))
        if len(requests) > 0:
            for request in requests:
                yield request

    def data_Extraction(self,response):

        # Grab all available text from the body, the information in the footer, and the identifier of the post.
        # Throw all text in a dictionary object.

        info = {}

        user = extract(response.xpath('/html/body/div[3]/a/text()'))
        info['user'] = user

        info['category'] = response.meta['category']

        title = extract(response.xpath('//div[@class="title"]/text()'))
        info['title'] = title

        substance = extract(response.xpath('//div[@class="substance"]/text()'))
        info['substance'] = substance

        doseChart = response.xpath('//table[@class="dosechart"]/tr')
        doses = []
        substances = []

        for row in doseChart:
            time = extract(row.xpath('td[1]/text()'))
            dose_size = extract(row.xpath('td[2]/text()'))
            ingestion_method = extract(row.xpath('td[3]/text()'))
            drug = extract(row.xpath('td[4]/a/text()'))
            if drug:
                substances.append(drug)
            dose = {'time':time,'dose_size': dose_size, 'ingestion_method': ingestion_method, 'drug': drug}
            if dose:
                doses.append(dose)

        info['substances'] = substances
        info['doses'] = doses

        body_text = []
        for i in range(50):
            paragraph_text = extract(response.xpath('//*[@class="report-text-surround"]/text()'+str([i])))
            if paragraph_text:
                body_text.append(paragraph_text)
        info['text'] = body_text

        body_weight = extract(response.xpath('//table[@class=bodyweight]/tr/td[2]/text()'))
        if body_weight:
            info['weight'] = body_weight

        rows = response.xpath('//table[@class="footdata"]/tr')

        fields = {
            'Exp Year': 'year',
            'ExpID': 'id',
            'Gender': 'gender',
            'Age at time of experience': 'age',
            'Published': 'published',
            'Views': 'views'
        }

        for row in rows:
            value = extract(row.xpath('td[1]/text()'))
            array = value.split(':')
            if len(array) > 1:
                if array[0] in fields:
                    outputField = fields[array[0]]
                    info[outputField] = array[1].strip()

            value = extract(row.xpath('td[2]/text()'))
            array = value.split(':')
            if len(array) > 1:
                if array[0] in fields:
                    outputField = fields[array[0]]
                    info[outputField] = array[1].strip()

        with open('drugs.csv', 'a') as f:
            w = csv.DictWriter(f, info.keys(), delimiter = ',', lineterminator='\n')
            w.writerow(info)
