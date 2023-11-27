import pandas
from argparse import ArgumentParser

parser = ArgumentParser(prog='add-weather', description='Add columns containing weather informations for both arrival and departure airports in airline delay csv')

parser.add_argument("-i", required=True,
                    help="Input CSV file", metavar='INPUT_FILE')
parser.add_argument("-o", required=True,
                    help="Output CSV file", metavar='OUTPUT_FILE')
parser.add_argument("-lo", required=True,
                    help="CSV file listing airports' IATA/ICAO and latlong", metavar='LOCATION_FILE')


args = parser.parse_args()

#airport_location =k
