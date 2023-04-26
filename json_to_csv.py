from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class JsonToCsv(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol
    INPUT_PROTOCOL = RawValueProtocol

    


    def mapper(self, _, line):

        def parseJSON(json):
            opening_brackets = []
            before_colon = True
            in_parenthesis = False
            words = []
            word = ""
            for character in json:

                # check for nested information which we dont want to parse
                if not in_parenthesis and character == '{':
                    opening_brackets.append(character)

                if len(opening_brackets) > 1:
                    word = word + character

                if not in_parenthesis and character == '}':
                    opening_brackets.pop()
                    if len(opening_brackets) == 1: 
                        words.append(word)
                        word = ""
                        before_colon = True
                
                elif len(opening_brackets) == 1:
                    if character == '"' and in_parenthesis == False:
                        in_parenthesis = True
                    elif character == '"' and in_parenthesis == True:
                        in_parenthesis = False

                        if before_colon == False: 
                            words.append(word)
                            before_colon = True
                        word = ""

                    elif not in_parenthesis and not before_colon and character == ',':
                        words.append(word)
                        before_colon = True
                        word = ""


                    elif in_parenthesis:
                        word = word + character

                    elif not in_parenthesis and not before_colon:
                        word = word + character 
                    
                    elif character == ":": before_colon = False

            return words

        text = line.replace(':null', ':"null"').replace('\\n','').replace('\\r', '')
        try:
            
            # dictionary = eval(text)
            # value = ''
            # for i, entry in enumerate(dictionary.values()):
            #     # No seperator after last line
            #     if(i+1 == len(dictionary)):
            #         value += f'"{entry}"'
            #     else:
            #         value += f'"{entry}"|'

            words = parseJSON(line)
            value = ''
            for i, entry in enumerate(words):
                # No seperator after last line
                if(i+1 == len(words)):
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