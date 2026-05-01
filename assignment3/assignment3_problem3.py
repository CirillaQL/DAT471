#!/usr/bin/env python3

from mrjob.job import MRJob


class MRJobTwitterFollows(MRJob):
    # The final (key,value) pairs returned by the class should be
    #
    # yield ('most followed id', ???)
    # yield ('most followed', ???)
    # yield ('average followed', ???)
    # yield ('count follows no-one', ???)
    #
    # You will, of course, need to replace ??? with a suitable expression

    def mapper(self, _, line):
        if not line.strip():
            return

        user_id_text, follows_text = line.split(":", 1)
        user_id = int(user_id_text)

        follows_text = follows_text.strip()
        if follows_text == "":
            follows_count = 0
        else:
            follows_count = sum(
                1 for follow_id in follows_text.split(",") if follow_id.strip()
            )

        yield None, (user_id, follows_count, 1, follows_count, int(follows_count == 0))

    def combiner(self, _, values):
        yield None, self.combine_values(values)

    def reducer(self, _, values):
        max_user_id, max_count, user_count, total_follow_count, no_follow_count = (
            self.combine_values(values)
        )

        yield "most followed id", max_user_id
        yield "most followed", max_count
        yield "average followed", total_follow_count / user_count
        yield "count follows no-one", no_follow_count

    @staticmethod
    def combine_values(values):
        max_user_id = None
        max_count = -1
        user_count = 0
        total_follow_count = 0
        no_follow_count = 0

        for (
            user_id,
            follows_count,
            partial_user_count,
            partial_total_follow_count,
            partial_no_follow_count,
        ) in values:
            if follows_count > max_count or (
                follows_count == max_count
                and (max_user_id is None or user_id < max_user_id)
            ):
                max_user_id = user_id
                max_count = follows_count

            user_count += partial_user_count
            total_follow_count += partial_total_follow_count
            no_follow_count += partial_no_follow_count

        return max_user_id, max_count, user_count, total_follow_count, no_follow_count

if __name__ == '__main__':
    MRJobTwitterFollows.run()
