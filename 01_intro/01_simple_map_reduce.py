from mrjob.job import MRJob


# aby zdef. joba budujemy klase dziedziczaca po klasie MRJOB
# jeden krok sklada sie z etapow; mapper, combiner, reducer i kazdy jest opcjonalny
# domyslnie job jest uruchomiony jako jeden proces


class MRWordCount(MRJob):  # process every line from argument file

    def mapper(self, _, line):
        yield 'chars', len(line)  # print each line length
        yield 'words', len(line.split())

    def reducer(self, key, values):  # reductor: sums up values - lengths of lines
        yield key, sum(values)


if __name__ == "__main__":
    MRWordCount.run()
