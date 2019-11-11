from mrjob.job import MRJob
from mrjob.step import MRStep

#usage of this script: >py 05_average_departure_arrival_delay_by_airline.py flights.csv flights.csv --airlines airlines.csv

class MRFlights(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer_init = self.reducer_init,
                   reducer=self.reducer
            )
        ]

    def configure_args(self):
        super(MRFlights, self).configure_args()
        self.add_file_arg('--airlines', help='path to the airlines.csv')

        
    def mapper(self, _, line):
        (year, month, day, day_of_week, airline, flight_number, tail_number, origin_airport,
         destination_airport, scheduled_departure, departure_time, departure_delay, taxi_out,
         wheels_off, scheduled_time, elapsed_time, air_time, distance, wheels_on, taxi_in,
         scheduled_arrival, arrival_time, arrival_delay, diverted, cancelled, cancellation_reason,
         air_system_delay, security_delay, airline_delay, late_aircraft_delay, weather_delay) = line.split(',')

        if departure_delay == '':
            departure_delay = 0
        if arrival_delay == '':
            arrival_delay = 0
        departure_delay = float(departure_delay)
        arrival_delay = float(arrival_delay)

        yield airline, (departure_delay, arrival_delay)


    def reducer_init(self):
        self.airline_names = {}
        with open('airlines.csv', 'r') as file:
            for line in file:
                code, fullname  = line.split(',')
                fullname = fullname[:-1]
                self.airline_names[code] = fullname



    def reducer(self, key, values):
        total_departure = 0
        total_arrival = 0
        num_elements = 0

        for value in values:
            total_departure += value[0]
            total_arrival += value[1]
            num_elements +=1

        yield self.airline_names[key], (total_departure/num_elements ,total_arrival/num_elements)  #key  == month



if __name__ == '__main__':
    MRFlights.run()
