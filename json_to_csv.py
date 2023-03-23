from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class JsonToCsv(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol
    INPUT_PROTOCOL = RawValueProtocol

    def mapper(self, _, line):
        text = line.replace(':null', ':"null"').replace('\\n','').replace('\\r', '')
        try:
            dictionary = eval(text)
            value = ''
            for i, entry in enumerate(dictionary.values()):
                # No seperator after last line
                if(i+1 == len(dictionary)):
                    value += f'"{entry}"'
                else:
                    value += f'"{entry}"|'

            yield None, value

        except:
            yield None, text[14:22] + '++++failed'
        #yield None, text

        

    # def combiner(self, key, values):
    #     yield key, sum(values)

    # def reducer(self, key, values):
    #     yield key, sum(values)


if __name__ == '__main__':
    JsonToCsv.run()