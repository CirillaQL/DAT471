from mrjob.job import MRJob
import csv

class StarMineralValue(MRJob):

    def mapper(self, _, line):
        # 跳过表头
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

    def combiner(self, star_system, values):
        yield star_system, sum(values)

    def reducer(self, star_system, values):
        yield star_system, sum(values)

if __name__ == "__main__":
    StarMineralValue.run()