#!/usr/bin/env python3

from heapq import nlargest
import csv

from mrjob.job import MRJob
from mrjob.step import MRStep


class MRMineralTopK(MRJob):

    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg(
            "-k",
            type=int,
            default=10,
            help="number of most valuable star systems to output",
        )

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_mineral_value,
                combiner=self.combiner_sum_values,
                reducer=self.reducer_sum_values,
            ),
            MRStep(reducer=self.reducer_top_k),
        ]

    def mapper_mineral_value(self, _, line):
        if line.startswith("Constellation,Star,Planet"):
            return

        row = next(csv.reader([line]))
        constellation = row[0]
        star = row[1]
        mineral_value = int(row[5])

        if star == "Prime":
            star_system = constellation
        else:
            star_system = f"{star} {constellation}"

        yield star_system, mineral_value

    def combiner_sum_values(self, star_system, values):
        yield star_system, sum(values)

    def reducer_sum_values(self, star_system, values):
        yield None, (sum(values), star_system)

    def reducer_top_k(self, _, star_values):
        k = self.options.k
        top_systems = nlargest(k, star_values, key=lambda item: (item[0], item[1]))

        for mineral_value, star_system in top_systems:
            yield star_system, mineral_value


if __name__ == "__main__":
    MRMineralTopK.run()
