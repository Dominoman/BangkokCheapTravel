import smtplib
from datetime import datetime
from typing import Any

import pandas as pd
import requests
import sqlalchemy as db
from yattag import Doc

import json
from config import *


def get_flight_data(fly_from: str, fly_to: str, date_from: str, date_to: str, nights_in_dst_from: int,
                    nights_in_dst_to: int, max_fly_duration: int, max_stopovers: int = 1, limit: int = 1000,
                    curr: str = "HUF") -> Any:
    url = f'https://api.tequila.kiwi.com/v2/search?fly_from={fly_from}&fly_to={fly_to}&date_from={date_from}&date_to={date_to}&nights_in_dst_from={nights_in_dst_from}&nights_in_dst_to={nights_in_dst_to}&max_fly_duration={max_fly_duration}&flight_type=round&curr={curr}&locale=hu&max_stopovers={max_stopovers}&limit={limit}'
    print(url)
    response = requests.get(url, headers={'apikey': APIKEY})
    if response.status_code != 200:
        print(f'{response.status_code} {response.text}')
    return response.json()


def sendmail(recipient: str, subject: str, content):
    headers = [f"From: {SMTP_USERNAME}", f"Subject: {subject}", f"To: {recipient}", "MIME-Version: 1.0",
               "Content-Type: text/html"]
    headers = "\r\n".join(headers)

    session = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
    session.ehlo()
    #session.starttls()
    #session.ehlo()
    if SMTP_USERNAME!="":
        session.login(SMTP_USERNAME, SMTP_PASSWORD)
    session.sendmail(SMTP_USERNAME, recipient, headers + "\r\n\r\n" + content)
    session.quit()


class Database:
    def __init__(self, path: str, echo: bool) -> None:
        self.engine = db.create_engine(f"sqlite:///{path}", echo=echo)
        self.conn = self.engine.connect()
        self.metadata = db.MetaData()
        self.itinerary = db.Table("Itinerary", self.metadata, autoload_with=self.engine)
        self.route = db.Table("Route", self.metadata, autoload_with=self.engine)
        self.carriers = self.get_carriers()
        self.itinerary_all = self.itinerary_add = self.route_all = self.route_add = 0

    def store_flight_data(self, date: datetime, data: dict) -> None:
        for itinerary in data:
            for route in itinerary['route']:
                self.store_route(route)

            self.itinerary_all += 1
            count = db.Select(db.func.count()).select_from(self.itinerary).where(
                self.itinerary.c.id == itinerary["id"]).where(self.itinerary.c.importdate == date.date())
            result = self.conn.scalar(count)
            if result == 0:
                local_arrival = datetime.strptime(itinerary["local_arrival"], "%Y-%m-%dT%H:%M:%S.000Z")
                local_departure = datetime.strptime(itinerary["local_departure"], "%Y-%m-%dT%H:%M:%S.000Z")
                insert = db.insert(self.itinerary).values(importdate=date.date(), id=itinerary["id"],
                                                          flyFrom=itinerary["flyFrom"], flyTo=itinerary["flyTo"],
                                                          cityFrom=itinerary["cityFrom"],
                                                          cityCodeFrom=itinerary["cityCodeFrom"],
                                                          cityTo=itinerary["cityTo"],
                                                          cityCodeTo=itinerary["cityCodeTo"],
                                                          countryFromCode=itinerary["countryFrom"]["code"],
                                                          countryFromName=itinerary["countryFrom"]["name"],
                                                          countryToCode=itinerary["countryTo"]["code"],
                                                          countryToName=itinerary["countryTo"]["name"],
                                                          nightsInDest=itinerary["nightsInDest"],
                                                          quality=itinerary["quality"], distance=itinerary["distance"],
                                                          durationDeparture=itinerary["duration"]["departure"],
                                                          durationReturn=itinerary["duration"]["return"],
                                                          durationTotal=itinerary["duration"]["total"],
                                                          price=itinerary["price"],
                                                          conversionEUR=itinerary["conversion"]["EUR"],
                                                          conversionHUF=itinerary["conversion"]["HUF"],
                                                          availabilitySeats=itinerary["availability"]["seats"],
                                                          airlines=",".join([self.get_carrier(carrier) for carrier in
                                                                             itinerary['airlines']]),
                                                          booking_token=itinerary["booking_token"],
                                                          deep_link=itinerary["deep_link"],
                                                          facilitated_booking_available=itinerary[
                                                              "facilitated_booking_available"],
                                                          pnr_count=itinerary["pnr_count"],
                                                          has_airport_change=itinerary["has_airport_change"],
                                                          technical_stops=itinerary["technical_stops"],
                                                          throw_away_ticketing=itinerary["throw_away_ticketing"],
                                                          hidden_city_ticketing=itinerary["hidden_city_ticketing"],
                                                          virtual_interlining=itinerary["virtual_interlining"],
                                                          local_arrival=local_arrival, local_departure=local_departure)
                self.conn.execute(insert)
                self.itinerary_add += 1
        self.conn.commit()

    def store_route(self, route: dict[str, Any]) -> None:
        self.route_all += 1
        count = db.Select(db.func.count()).select_from(self.route).where(self.route.c.id == route["id"])
        result = self.conn.scalar(count)
        if result == 0:
            local_arrival = datetime.strptime(route["local_arrival"], "%Y-%m-%dT%H:%M:%S.000Z")
            local_departure = datetime.strptime(route["local_departure"], "%Y-%m-%dT%H:%M:%S.000Z")
            insert = db.Insert(self.route).values(id=route['id'], combination_id=route['combination_id'],
                                                  flyFrom=route['flyFrom'], flyTo=route['flyTo'],
                                                  cityFrom=route['cityFrom'], cityCodeFrom=route['cityCodeFrom'],
                                                  cityTo=route['cityTo'], cityCodeTo=route['cityCodeTo'],
                                                  airline=route['airline'], flight_no=route['flight_no'],
                                                  operating_carrier=route['operating_carrier'],
                                                  operating_flight_no=route['operating_flight_no'],
                                                  fare_basis=route['fare_basis'],
                                                  fare_category=route['fare_category'],
                                                  fare_classes=route['fare_classes'],
                                                  fare_family=route['fare_family'], _return=route['return'],
                                                  bags_recheck_required=route['bags_recheck_required'],
                                                  vi_connection=route['vi_connection'], guarantee=route['guarantee'],
                                                  equipment=route['equipment'], vehicle_type=route['vehicle_type'],
                                                  local_arrival=local_arrival, local_departure=local_departure)
            self.conn.execute(insert)
            self.route_add += 1

    def get_carrier(self, carrier_id: str) -> str:
        return next(
            (row['name'] for row in self.carriers if row['id'] == carrier_id), ""
        )

    @staticmethod
    def get_carriers():
        url = 'https://tequila-api.kiwi.com/carriers'
        response = requests.get(url)
        return response.json()

    def get_last_cheap_flight(self, limit: int):
        # select * from "Itinerary" where importdate = (select max(importdate) from "Itinerary") order by price desc limit 5
        subselect = db.Select(db.func.max(self.itinerary.c.importdate)).scalar_subquery()
        select = db.Select(self.itinerary).where(self.itinerary.c.importdate == subselect).order_by(
            self.itinerary.c.price.desc()).limit(limit)
        data = pd.read_sql(select, self.conn)
        return data.to_html()


# flight_data = get_flight_data("VIE,BUD", "BKK", "26/12/2023", "31/12/2023", 7, 14, 15)

# with open('json/20231003BKK.json', 'r') as fo:
#     flight_data = json.load(fo)
# if 'data' in flight_data.keys():
#     database = Database(DB_PATH, DB_ECHO)
#     database.store_flight_data(datetime.now(), flight_data['data'])
#     print(
#         f"Itinerary:{database.itinerary_all}/{database.itinerary_add}, Route:{database.route_all}/{database.route_add}")

date = datetime.now()
database = Database(DB_PATH, DB_ECHO)

doc, tag, text = Doc().tagtext()
with tag('html'):
    with tag('body'):
        with tag('h1'):
            text('Repjegy keresés')
        with tag('p'):
            text(f'Lekérdezés dátuma: {date.strftime("%Y-%m-%d")}')
            text(database.get_last_cheap_flight(5))

result = doc.getvalue()
sendmail("laszlo.oravecz@gmail.com", "Finder", result)
print(result)
