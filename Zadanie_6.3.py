import os
import csv
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, Date
from sqlalchemy.orm import sessionmaker, declarative_base

# Pliki CSV
stations_csv_path = r'clean_stations.csv'
measure_csv_path = r'clean_measure.csv'

# Baza danych
DATABASE_URL = "sqlite:///stations.db"  # Poprawiona nazwa

# Tworzymy bazę ORM
Base = declarative_base()

# Model tabeli stacji
class Station(Base):
    __tablename__ = 'stations'  # Poprawiona nazwa
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
    station = Column(String, ForeignKey('stations.station'), nullable=False)  # Poprawiona relacja
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
        session = Session()
        try:
            session.bulk_save_objects(stations_data)
            session.commit()
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

# Główne wywołanie programu
if __name__ == "__main__":
    # Usuwamy starą bazę danych, jeśli istnieje
    if os.path.exists("stations.db"):  
        os.remove("stations.db")

    # Tworzymy nową bazę i tabele
    Base.metadata.create_all(engine)

    # Importujemy dane
    import_stations(stations_csv_path)
    import_measurements(measure_csv_path)

    # Sprawdzamy dane przez ORM
    with Session() as session:
        stations = session.query(Station).limit(5).all()
        for station in stations:
            print(station.id, station.name, station.country)
