import os
import csv
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, Date
from sqlalchemy.orm import sessionmaker, declarative_base

# Pliki CSV
stations_csv_path = r'clean_stations.csv'
measure_csv_path = r'clean_measure.csv'

# Baza danych
DATABASE_URL = "sqlite:///stations.db"

# Tworzymy bazę ORM
Base = declarative_base()

# Model tabeli stacji
class Station(Base):
    __tablename__ = 'stations'
    id = Column(Integer, primary_key=True, autoincrement=True)
    station = Column(String, unique=True, nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    elevation = Column(Float, nullable=False)
    name = Column(String, nullable=False)
    country = Column(String, nullable=False)
    state = Column(String, nullable=True)

# Model tabeli pomiarów
class Measurement(Base):
    __tablename__ = 'measurements'
    id = Column(Integer, primary_key=True, autoincrement=True)
    station = Column(String, ForeignKey('stations.station'), nullable=False)
    date = Column(Date, nullable=False)
    precip = Column(Float, nullable=False)
    tobs = Column(Float, nullable=False)

# Tworzenie silnika bazy danych
engine = create_engine(DATABASE_URL, echo=False)
Session = sessionmaker(bind=engine)

# Tworzymy tabele w bazie danych
Base.metadata.create_all(engine)

# Funkcja do importu stacji
def import_stations(stations_csv_path):
    try:
        with open(stations_csv_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            stations_data = [
                Station(
                    station=row['station'],
                    latitude=float(row['latitude']),
                    longitude=float(row['longitude']),
                    elevation=float(row['elevation']),
                    name=row['name'],
                    country=row['country'],
                    state=row['state']
                )
                for row in reader
            ]
        session = Session()   # tworzy połączenie z bazą danych.Dopiero teraz możemy dodawać, pobierać i modyfikować dane.
        try:
            session.bulk_save_objects(stations_data)  # Dodaje wiele obiektów do sesji (ale jeszcze nie zapisuje ich w bazie).
            session.commit() #Zatwierdza transakcję, czyli zapisuje wszystkie zmiany w bazie. Bez commit(), dane byłyby jedynie w pamięci i nie zostałyby zapisane do pliku SQLite.
            print(f"Zaimportowano {len(stations_data)} stacji.")
        except Exception as e:
            session.rollback()
            print(f"Błąd zapisu do bazy: {e}")
        finally:
            session.close()
    except FileNotFoundError:
        print(f"Błąd: Nie znaleziono pliku {stations_csv_path}")
    except ValueError as e:
        print(f"Błąd wartości w pliku {stations_csv_path}: {e}")
    except Exception as e:
        print(f"Wystąpił nieoczekiwany błąd: {e}")

# Funkcja do importu pomiarów
def import_measurements(measure_csv_path):
    try:
        with open(measure_csv_path, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            measurements = [
                Measurement(
                    station=row['station'],
                    date=datetime.strptime(row['date'], "%Y-%m-%d").date(),  
                    precip=float(row['precip']),
                    tobs=float(row['tobs'])
                )
                for row in reader
            ]
        session = Session()
        try:
            session.bulk_save_objects(measurements)
            session.commit()
            print(f"Zaimportowano {len(measurements)} pomiarów.")
        except Exception as e:
            session.rollback()
            print(f"Błąd zapisu do bazy: {e}")
        finally:
            session.close()
    except FileNotFoundError:
        print(f"Błąd: Nie znaleziono pliku {measure_csv_path}")
    except ValueError as e:
        print(f"Błąd wartości w pliku {measure_csv_path}: {e}")
    except Exception as e:
        print(f"Wystąpił nieoczekiwany błąd: {e}")

# Dodanie podstawowych operacji SQLAlchemy
def insert_station(station, latitude, longitude, elevation, name, country, state):
    session = Session()
    new_station = Station(
        station=station,
        latitude=latitude,
        longitude=longitude,
        elevation=elevation,
        name=name,
        country=country,
        state=state
    )
    try:
        session.add(new_station)
        session.commit()
        print(f"Dodano stację: {name}")
    except Exception as e:
        session.rollback()
        print(f"Błąd dodawania stacji: {e}")
    finally:
        session.close()

def update_station_name(station_code, new_name):
    session = Session()
    try:
        station = session.query(Station).filter_by(station=station_code).first()
        if station:
            station.name = new_name
            session.commit()
            print(f"Zaktualizowano nazwę stacji {station_code} na {new_name}")
        else:
            print("Stacja nie znaleziona.")
    except Exception as e:
        session.rollback()
        print(f"Błąd aktualizacji: {e}")
    finally:
        session.close()

def get_stations_by_country(country):
    session = Session()
    try:
        stations = session.query(Station).filter_by(country=country).all()
        for station in stations:
            print(station.id, station.name, station.state)
    except Exception as e:
        print(f"Błąd pobierania danych: {e}")
    finally:
        session.close()

def delete_station(station_code):
    session = Session()
    try:
        station = session.query(Station).filter_by(station=station_code).first()
        if station:
            session.delete(station)
            session.commit()
            print(f"Usunięto stację: {station_code}")
        else:
            print("Stacja nie znaleziona.")
    except Exception as e:
        session.rollback()
        print(f"Błąd usuwania: {e}")
    finally:
        session.close()


if __name__ == "__main__":
    # Zamykamy połączenie z bazą przed usunięciem pliku
    engine.dispose()

    if os.path.exists("stations.db"):  
        try:
            os.remove("stations.db")
            print("Usunięto starą bazę danych.")
        except PermissionError:
            print("Nie można usunąć pliku bazy danych. Upewnij się, że nie jest otwarty w innym programie.")

    # Tworzymy nową bazę i tabele
    engine = create_engine(DATABASE_URL, echo=False)
    Base.metadata.create_all(engine)

    import_stations(stations_csv_path)
    import_measurements(measure_csv_path)

    insert_station("TEST001", 52.23, 21.01, 100, "Test Station", "Poland", "Mazowieckie")
    update_station_name("TEST001", "Updated Test Station")
    get_stations_by_country("Poland")
    delete_station("TEST001")
