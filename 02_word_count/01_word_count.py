from mrjob.job import MRJob
from mrjob.step import MRStep


class MRWordCount(MRJob):

    def steps(self):  # do kontrolowania krokow - funkcji ktore sie wywoluja, mozna zakomentowac elementy odpowiadajace poszczegolnym funkcjom
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer
                   )
        ]

    def mapper(self, _, line):
        words = line.split()
        for word in words:
            yield word.lower(), 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRWordCount.run()
