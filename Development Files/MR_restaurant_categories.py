import re
from mrjob.job import MRJob

def extract_between(key1, key2, text):
        text = text[text.find(f'{key1}')+ len(key1): text.find(key2)]
        return text.replace('"','').replace(':','').replace('\n', '')

class MyMRJob(MRJob):

    def mapper(self, _, line):
        key_key = 'business_id'
        value_keys = ['name', 'address', 'city', 'state', 'postal_code', 'latitude',
                      'longitude', 'stars', 'review_count', 'is_open', 'attributes',
                      'categories', 'categories', 'hours']

        key = extract_between(key_key, value_keys[0], line)[:-1]

        value = []
        for i, value_key in enumerate(value_keys):
            if(i+1 == len(value_keys)):
                value.append(extract_between(value_key, '}', line))
            
            else:
                value.append(extract_between(value_key, value_keys[i+1], line)[:-1])


        
        #line = line.strip() # remove leading and trailing whitespace
        # business_id = line[line.find('"business_id":"')+15: line.find('","name')]
        # name = line[line.find('"name":"')+8: line.find('","address')]
        # address = line[line.find('"address":"')+11: line.find('","city')]
        # city = line[line.find('"city":"')+8: line.find('","state')]
        # state = line[line.find('"state":"')+9: line.find('","postal_code')]
        # postal_code = line[line.find('"postal_code":"')+15: line.find('","latitude')]
        # latitude= line[line.find('"latitude":')+11: line.find(',"longitude')]
        # longitude= line[line.find('longitude":')+11: line.find(',"stars"')]
        # stars= line[line.find('"stars":')+8: line.find(',"review_count":')]
        # review_count= line[line.find('"review_count":')+15: line.find(',"is_open":')]
        # is_open= line[line.find('"is_open":')+10: line.find(',"attributes":')]
        # attributes= line[line.find('"attributes":')+13: line.find(',"categories":"')]
        # categories = line[line.find('"categories":"')+14: line.find('","hours')]
        # hours= line[line.find('"hours"')+7: line.find('}')]
        #business_id = re.search(r'"business_id":"(.*?)"', line).group(1)
        #categories = re.search(r'"categories":"(.*?)"', line).group(1)
        # text = line[4]
        yield key, value #(name, address, city,state, postal_code,latitude, longitude, stars, review_count, is_open, categories, hours)
        # yield text, 1

    # def combiner(self, key, values):
    #     yield key, sum(values)

    # def reducer(self, key, values):
    #     yield key, sum(values)


if __name__ == '__main__':
    MyMRJob.run()