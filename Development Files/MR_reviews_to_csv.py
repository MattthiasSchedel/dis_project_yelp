import re
from mrjob.job import MRJob

def extract_between(key1, key2, text):
        text = text[text.find(f'{key1}')+ len(key1): text.find(key2)]
        return text.replace('"','').replace(':','')

class toCSV(MRJob):

    def mapper(self, _, line):
        key_key = 'review_id'
        value_keys = ['review_id','user_id', 'business_id', 'stars', 'useful', 'funny', 'cool',
                      'text', 'date']

        key = extract_between(key_key, value_keys[0], line)[:-1]

        value = ''
        for i, value_key in enumerate(value_keys):
            if(i+1 == len(value_keys)):
                #value.append(extract_between(value_key, '}', line))
                text = extract_between(value_key, '}', line)
                value += text
            
            else:
                #value.append(extract_between(value_key, value_keys[i+1], line)[:-1])
                text = extract_between(value_key, value_keys[i+1], line)[:-1]
                value += f'{text},'
    
        #yield key, tuple(value)
        yield None, value

    # def combiner(self, key, values):
    #     yield key, sum(values)

    # def reducer(self, key, values):
    #     yield key, sum(values)


if __name__ == '__main__':
    toCSV.run()