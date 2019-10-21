from mrjob.job import MRJob
from mrjob.step import MRStep


class MRSimpleJob(MRJob):

    def steps(self): #do kontrolowania krokow - funkcji ktore sie wywoluja
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        yield 'lines', 1
        yield 'words', len(line.split())
        yield 'chars', len(line)

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRSimpleJob.run()
