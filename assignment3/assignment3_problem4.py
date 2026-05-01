#!/usr/bin/env python3

from mrjob.job import MRJob
from mrjob.step import MRStep


class MRJobTwitterFollowers(MRJob):
    # The final (key,value) pairs returned by the class should be
    #
    # yield ('most followers id', ???)
    # yield ('most followers', ???)
    # yield ('average followers', ???)
    # yield ('count no followers', ???)
    #
    # You will, of course, need to replace ??? with a suitable expression

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_followers,
                combiner=self.combiner_sum_followers,
                reducer=self.reducer_sum_followers,
            ),
            MRStep(
                combiner=self.combiner_stats,
                reducer=self.reducer_stats,
            ),
        ]

    def mapper_followers(self, _, line):
        if not line.strip():
            return

        user_id_text, follows_text = line.split(":", 1)
        user_id = int(user_id_text)

        yield user_id, 0

        follows_text = follows_text.strip()
        if follows_text == "":
            return

        for followed_id_text in follows_text.replace(",", " ").split():
            followed_id_text = followed_id_text.strip()
            if followed_id_text:
                yield int(followed_id_text), 1

    def combiner_sum_followers(self, user_id, follower_counts):
        yield user_id, sum(follower_counts)

    def reducer_sum_followers(self, user_id, follower_counts):
        follower_count = sum(follower_counts)
        yield None, (user_id, follower_count, 1, follower_count, int(follower_count == 0))

    def combiner_stats(self, _, values):
        yield None, self.combine_stats(values)

    def reducer_stats(self, _, values):
        max_user_id, max_count, user_count, total_followers, no_followers_count = (
            self.combine_stats(values)
        )

        yield "most followers id", max_user_id
        yield "most followers", max_count
        yield "average followers", total_followers / user_count
        yield "count no followers", no_followers_count

    @staticmethod
    def combine_stats(values):
        max_user_id = None
        max_count = -1
        user_count = 0
        total_followers = 0
        no_followers_count = 0

        for (
            user_id,
            follower_count,
            partial_user_count,
            partial_total_followers,
            partial_no_followers_count,
        ) in values:
            if follower_count > max_count or (
                follower_count == max_count
                and (max_user_id is None or user_id < max_user_id)
            ):
                max_user_id = user_id
                max_count = follower_count

            user_count += partial_user_count
            total_followers += partial_total_followers
            no_followers_count += partial_no_followers_count

        return max_user_id, max_count, user_count, total_followers, no_followers_count


if __name__ == "__main__":
    MRJobTwitterFollowers.run()
