from mrjob.job import MRJob
from mrjob.step import MRStep

class TopMoviesSorted(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movieIDs,
                   reducer=self.reducer_count_movieIDs),
            MRStep(reducer=self.reducer_sorted_output)
        ]

    def mapper_get_movieIDs(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_movieIDs(self, key, values):
        yield str(sum(values)).zfill(5), key

    def reducer_sorted_output(self, count, movies):
        for movie in movies:
            yield movie, count


if __name__ == '__main__':
    TopMoviesSorted.run()
