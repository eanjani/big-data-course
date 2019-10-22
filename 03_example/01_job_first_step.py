from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r'[\w]+')

class MRJobFirstStep(MRJob):

    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                   combiner = self.combiner,
                   reducer = self.reducer)
        ]


    def mapper(self, _, line):
        words = WORD_RE.findall(line)
        for word in words:
            yield word.lower(), 1


    def combiner(self, key, values): #combines the same words with sum of occurences locally - in line
        yield key, sum(values)

    def reducer(self, key, values): #combines words with occurences globally - in file
        yield key, sum(values)



if __name__ == '__main__':
    MRJobFirstStep.run()

