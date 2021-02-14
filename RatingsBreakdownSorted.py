from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdownSorted(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_sorted_output)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield rating, 1

    def reducer_count_ratings(self, key, values):
        yield str(sum(values)).zfill(5), key

    def reducer_sorted_output(self, count, ratings):
        for rating in ratings:
            yield rating, count


if __name__ == '__main__':
    RatingsBreakdownSorted.run()
