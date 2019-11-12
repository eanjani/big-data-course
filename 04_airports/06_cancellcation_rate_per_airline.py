from mrjob.job import MRJob
from mrjob.step import MRStep


class MRFlights(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        (year, month, day, day_of_week, airline, flight_number, tail_number, origin_airport,
         destination_airport, scheduled_departure, departure_time, departure_delay, taxi_out,
         wheels_off, scheduled_time, elapsed_time, air_time, distance, wheels_on, taxi_in,
         scheduled_arrival, arrival_time, arrival_delay, diverted, cancelled, cancellation_reason,
         air_system_delay, security_delay, airline_delay, late_aircraft_delay, weather_delay) = line.split(',')

        yield airline, int(cancelled)

    def configure_args(self):
        super(MRFlights, self).configure_args()
        self.add_file_arg('--airlines', help='path to the airlines.csv')

    def reducer_init(self):
        self.airline_names = {}
        with open('airlines.csv', 'r') as file:
            for line in file:
                code, fullname = line.split(',')
                fullname = fullname[:-1]
                self.airline_names[code] = fullname

    def reducer(self, key, values):
        total = 0
        num_rows = 0
        for val in values:
            total += val
            num_rows += 1
        yield self.airline_names[key], total / num_rows


if __name__ == '__main__':
    MRFlights.run()
