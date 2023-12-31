from datetime import datetime, date, timedelta
from airflow.datasets import Dataset
from airflow.decorators import task, dag
from datetime import datetime, date
import pandas as pd
from pandas.api.types import is_string_dtype, is_numeric_dtype, is_datetime64_any_dtype, is_bool_dtype, is_integer_dtype
import os
from help_func import show_new_cols
import sys
import subprocess

nationwide_data = Dataset(r'./rawdata/August 2018 Nationwide.csv')
air_carriers_data = Dataset(r'./rawdata/Air Carriers')
airports_data = Dataset(r'./rawdata/Airports')

date_transformed_data = Dataset("./curated/date_tranformed.csv")
date_transformed_data_new = Dataset("./curated/date_tranformed_new.csv")

air_carriers_transformed_data = Dataset("./curated/air_carriers_transformed_data.csv")
air_carriers_transformed_data_new = Dataset("./curated/air_carriers_transformed_data_new.csv")

airports_transformed_data = Dataset("./curated/airports_transformed_data.csv")
airports_transformed_data_new = Dataset("./curated/airports_transformed_data_new.csv")

cancelations_transformed_data = Dataset("./curated/cancelations_transformed_data.csv")
cancelations_transformed_data_new = Dataset("./curated/cancelations_transformed_data_new.csv")

time_transformed_data = Dataset("./curated/time_transformed_data.csv")
time_transformed_data_new = Dataset("./curated/time_transformed_data_new.csv")

delay_transformed_data = Dataset("./curated/delay_transformed_data.csv")
delay_transformed_data_new = Dataset("./curated/delay_transformed_data_new.csv")

flights_transformed_data = Dataset("./curated/flights_transformed_data.csv")
flights_transformed_data_new = Dataset("./curated/flights_transformed_data_new.csv")


# A DAG represents a workflow, a collection of tasks
@dag(dag_id="load_and_transform", start_date=datetime(2023, 6, 11), schedule="@once")
def etl_process():
    # extracting source file
    @task(inlets=[nationwide_data])
    def load_data_nationwide():

        df = pd.read_csv(nationwide_data.uri)
        return df

    @task(inlets=[air_carriers_data])
    def load_air_carriers():

        df = pd.read_csv(air_carriers_data.uri)

        return df

    @task(inlets=[airports_data])
    def load_data_airports():

        df = pd.read_csv(airports_data.uri)
        return df

    # transforming part
    @task()
    def tranform_airports(df: pd.DataFrame):
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'geopy'])

        def name_col(row):
            if not row or ':' not in row:
                return 'Not specified'

            return row.split(':')[-1].strip()

        def city_col(row):
            if not row or ',' not in row:
                return 'Not specified'

            return row.split(',')[0].strip()

        def country_col(row):

            if not row or ':' not in row:
                return 'Not specified'

            country = row.split(':')[0].split(',')[1].strip()
            if len(country) > 2:
                return country
            else:
                return 'United States'

        df['name'] = df.Description.apply(name_col)
        df['city'] = df.Description.apply(city_col)
        df['country'] = df.Description.apply(country_col)
        df["airport_id_pk"] = df.index
        df.rename(columns={'Code': 'airport_code'}, inplace=True)

        df.drop(columns=['Description'], inplace=True)

        df = df[['airport_id_pk', 'airport_code', 'name', 'city', 'country']]
        df.drop_duplicates(inplace=True)
        return df

    @task()
    def tranform_air_carriers(df: pd.DataFrame):

        def name_col(row):

            if not row or ',' not in row:
                return 'Not specified'

            return row.split(',')[0]

        def shortcut_col(row):

            if not row or ':' not in row:
                return 'Not specified'

            return row.split(':')[1]

        df['name'] = df.Description.apply(name_col)
        df['shortcut'] = df.Description.apply(shortcut_col)

        df.drop(columns=['Description'], inplace=True)
        df.columns = ['air_carrier_id_pk', 'name', 'shortcut']

        return df

    @task()
    def transform_date_table(df: pd.DataFrame):
        def add_date(new_date, date_table_dict):
            # dodaje nową datę do dict date_table_dict, gdzie new_date to string w formacie YYYY-MM-DD, a date_table_dict
            # przechowuje elementy, które trafią do dataframe

            date_now = datetime.now()  # wyłącznie do porównania czy rok w new_date nie jest większy od obecnego
            year, month, day = (int(x) for x in new_date.split('-'))

            new_date_1 = date(year, month, day)

            if year >= date_now.year:
                print("Wrong year")
            if month not in [item for item in range(1, 12)]:
                print("Wrong month")
            if day not in [item for item in range(1, 31)]:
                print("Wrong day")

            day_of_week = new_date_1.weekday()
            prev_id = max(date_table_dict["date_id_pk"]) if date_table_dict["date_id_pk"] else -1
            new_id = prev_id + 1
            is_workday = 1
            is_weekend = 1
            if day_of_week > 4:  # poniedziałek to 0, niedziela to 6
                is_workday = False
                is_weekend = True
            else:
                is_workday = True
                is_weekend = False

            # to, co chcę dorzucić do słownika
            new_date_list = [new_id, day, month, year, is_workday, is_weekend, int(month / 4) + 1, new_date]

            date_table_keys = list(date_table_dict.keys())
            # petla do aktualizacji wartości w słowniku
            for i in range(len(date_table_keys)):
                key = date_table_keys[i]
                values = list(date_table_dict[key])
                values.append(new_date_list[i])
                date_table_dict[key] = values
            return date_table_dict

        def create_date_frame():
            # tworzę szkielet słownika, który, po uzupełnieniu danymi, zostanie przekształcony na dataframe
            date_table_dict = {
                "date_id_pk": [],
                "day": [],
                "month": [],
                "year": [],
                "is_work_day": [],
                "is_weekday": [],
                "quarter": [],
                "full_date": []
            }

            list_dates = df['FL_DATE'].tolist()  # zbieram daty z pliku do listy
            list_dates = list(dict.fromkeys(list_dates))  # i usuwam duplikaty

            for d in list_dates[:10]:  # dodaję elementy z listy do słownika
                date_table_dict = add_date(d, date_table_dict)

            date_table = pd.DataFrame(date_table_dict)

            return date_table

        return create_date_frame()

    @task()
    def transform_time_table(df: pd.DataFrame):
        def create_time_frame():

            # tworzę szkielet słownika, który, po uzupełnieniu danymi, zostanie przekształcony na dataframe
            time_table_dict = {
                "time_id_pk": [],
                "full_time": [],
                "hour": [],
                "time_of_the_day": []
            }

            # słownik, który służy do określania pór dnia
            time_of_day_names = {
                "night": [22, 23] + [item for item in range(0, 6)],
                "morning": [item for item in range(6, 12)],
                "afternoon": [item for item in range(12, 18)],
                "evening": [item for item in range(18, 22)],
            }
            time_table_keys = list(time_table_dict.keys())

            # tworzę listę wszystkich godzin w dobie z krokiem (delta) co 1 minutę
            start_dt = datetime(2022, 6, 10)
            end_dt = datetime(2022, 6, 11)
            delta = timedelta(minutes=1)
            times = []

            while start_dt < end_dt:
                times.append(str(start_dt.hour) + ":" + str(start_dt.minute))
                start_dt += delta

            # na bazie listy godzin dodaję elementy do słownika
            for t in times:
                hour, minutes = (int(x) for x in t.split(':'))
                time_id = hour * 60 + minutes  # id to ilość minut od początku doby

                # zamieniam na string hodziny i minuty, aby elementy w full_time były w formacie HH:MM
                str_hour = str(hour)
                if len(str_hour) < 2:
                    str_hour = '0' + str_hour
                str_min = str(minutes)
                if len(str_min) < 2:
                    str_min = '0' + str_min
                full_time = str_hour + ":" + str_min

                # na podstawie godziny ustalam porę dnia
                time_of_day = [key for key, val in time_of_day_names.items() if hour in val][0]

                # lista tego, co chcę dorzucić do słownika
                time_list = [time_id, full_time, hour, time_of_day]

                # w pętli aktualizuję dane ze słownika
                for i in range(len(time_table_keys)):
                    key = time_table_keys[i]
                    values = list(time_table_dict[key])
                    values.append(time_list[i])
                    time_table_dict[key] = values

            time_table = pd.DataFrame(time_table_dict)
            return time_table

        return create_time_frame()

    @task()
    def transform_cancelations_table(df: pd.DataFrame):

        df = df[["CANCELLED", "CANCELLATION_CODE"]]
        df.drop_duplicates(inplace=True)

        df = df.rename_axis('cancelation_id_pk').reset_index()
        df.rename(columns={"CANCELLED": 'is_canceled', 'CANCELLATION_CODE': 'cancellation_code'}, inplace=True)

        df = df[['cancelation_id_pk', 'is_canceled', 'cancellation_code']]

        return df

    @task()
    def transform_delays_table(df: pd.DataFrame):

        original_cols = ["CRS_ELAPSED_TIME", "ACTUAL_ELAPSED_TIME", "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY",
                         "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"]
        df = df[original_cols]
        df = df.astype('float64')

        df["ACTUAL_ELAPSED_TIME"].fillna(df["CRS_ELAPSED_TIME"], inplace=True)
        df.fillna(0, inplace=True)
        df['other_type_delay'] = df["ACTUAL_ELAPSED_TIME"] - df['CRS_ELAPSED_TIME']
        df.drop(columns=['CRS_ELAPSED_TIME', 'ACTUAL_ELAPSED_TIME'], inplace=True)

        rename_dict = {name: name.lower() for name in original_cols[2:]}
        df.rename(columns=rename_dict, inplace=True)

        df.drop_duplicates(inplace=True)

        df = df.rename_axis('delay_id_pk').reset_index()

        return df

    @task()
    def transform_flight_table(main_df, airports_df, dates_df, delay_df):
        # merging part
        df = pd.merge(main_df, airports_df[['airport_code', "airport_id_pk"]], how='left', left_on='ORIGIN',
                      right_on='airport_code', suffixes=('', '')).drop(columns='airport_code')
        df.rename(columns={'airport_id_pk': 'arrival_airport_id_fk'}, inplace=True)

        df = pd.merge(df, airports_df[['airport_code', "airport_id_pk"]], how='left', left_on='ORIGIN',
                      right_on='airport_code', suffixes=('', '')).drop(columns='airport_code')
        df.rename(columns={'airport_id_pk': 'destination_airport_id_fk'}, inplace=True)

        df = pd.merge(df, dates_df[['full_date', "date_id_pk"]], how='left', left_on='ORIGIN', right_on='full_date',
                      suffixes=('', '')).drop(columns='full_date')
        df.rename(columns={'date_id_pk': 'date_id_fk'}, inplace=True)

        df.rename(columns={'OP_CARRIER_AIRLINE_ID': "air_carrier_id_fk"})

        time_cols = ['departure_time_fk', 'departure_final_time_fk', 'arrival_time_fk', 'arrivel_final_time_fk']

        df["ARR_TIME"].fillna(0, inplace=True)
        df['ARR_DELAY'].fillna(0, inplace=True)
        df["ARR_TIME"] = df["ARR_TIME"].astype('int64')
        df["ARR_DELAY"] = df["ARR_DELAY"].astype('int64')
        df['delay_id_fk'] = df.index
        df['CRS_ARR_TIME'] = df["ARR_TIME"] + (df['ARR_DELAY'] // 60) * 100 + df['ARR_DELAY'] % 60
        original_time_cols = ['CRS_DEP_TIME', "DEP_TIME", "ARR_TIME", 'CRS_ARR_TIME']
        for col_name, original_name in zip(time_cols, original_time_cols):
            df[original_name].fillna(0, inplace=True)
            df[original_name] = df[original_name].astype('int64')
            df[original_name] = df[original_name] % 60 + (df[original_name] // 100) * 60
            df.rename(columns={original_name: col_name}, inplace=True)

        df.drop(columns=['FL_DATE', "OP_CARRIER_AIRLINE_ID", "TAIL_NUM", "OP_CARRIER_FL_NUM", "ORIGIN_AIRPORT_ID",
                         "ORIGIN_AIRPORT_SEQ_ID", "ORIGIN_CITY_MARKET_ID", "ORIGIN", "DEST_AIRPORT_ID",
                         "DEST_AIRPORT_SEQ_ID", "DEST_CITY_MARKET_ID", "DEST", "DEP_DELAY", 'ARR_DELAY', 'CANCELLED',
                         'CANCELLATION_CODE', 'CRS_ELAPSED_TIME', 'ACTUAL_ELAPSED_TIME', 'CARRIER_DELAY',
                         'WEATHER_DELAY', 'NAS_DELAY', 'SECURITY_DELAY', "LATE_AIRCRAFT_DELAY", 'Unnamed: 28'],
                inplace=True)
        df.rename(columns={'DEP_DELAY_NEW': 'departure_delay', 'ARR_DELAY_NEW': 'arrival_delay'}, inplace=True)

        return df

    @task()
    def add_changes_to_date_table(df: pd.DataFrame):
        # mamy tabele ..new.csv aby do bazy danych wysłać tylko nowe rekody
        if not os.path.exists(date_transformed_data.uri):

            df.to_csv(date_transformed_data.uri, index=False)
            df.to_csv(date_transformed_data_new.uri, index=False)

        else:

            source = pd.read_csv(date_transformed_data.uri)
            new_data = show_new_cols(source, df)  # fukncja znajdująca nowe kolumny

            if new_data.empty:  # jeśli istnieją zmiany to je dodaj do pliku
                new_data.to_csv(date_transformed_data_new.uri, index=False)

                source.append(new_data)
                source.to_csv(date_transformed_data.uri, index=False)

        return df

    @task()
    def add_changes_to_air_carriers_table(df: pd.DataFrame):
        # mamy tabele ..new.csv aby do bazy danych wysłać tylko nowe rekody
        if not os.path.exists(air_carriers_transformed_data.uri):

            df.to_csv(air_carriers_transformed_data.uri, index=False)
            df.to_csv(air_carriers_transformed_data_new.uri, index=False)

        else:

            source = pd.read_csv(air_carriers_transformed_data.uri)
            new_data = show_new_cols(source, df)  # fukncja znajdująca nowe kolumny

            if new_data.empty:  # jeśli istnieją zmiany to je dodaj do pliku
                new_data.to_csv(air_carriers_transformed_data_new.uri, index=False)

                source.append(new_data)
                source.to_csv(air_carriers_transformed_data.uri, index=False)

        return df

    @task()
    def add_changes_to_airports_table(df: pd.DataFrame):
        # mamy tabele ..new.csv aby do bazy danych wysłać tylko nowe rekody
        if not os.path.exists(airports_transformed_data.uri):

            df.to_csv(airports_transformed_data.uri, index=False)
            df.to_csv(airports_transformed_data_new.uri, index=False)

        else:

            source = pd.read_csv(airports_transformed_data.uri)
            new_data = show_new_cols(source, df)  # fukncja znajdująca nowe kolumny

            if new_data.empty:  # jeśli istnieją zmiany to je dodaj do pliku
                new_data.to_csv(airports_transformed_data_new.uri, index=False)

                source.append(new_data)
                source.to_csv(airports_transformed_data.uri, index=False)

        return df

    @task()
    def add_changes_to_cancelations_table(df: pd.DataFrame):
        # mamy tabele ..new.csv aby do bazy danych wysłać tylko nowe rekody
        if not os.path.exists(cancelations_transformed_data.uri):

            df.to_csv(cancelations_transformed_data.uri, index=False)
            df.to_csv(cancelations_transformed_data_new.uri, index=False)

        else:

            source = pd.read_csv(cancelations_transformed_data.uri)
            new_data = show_new_cols(source, df)  # fukncja znajdująca nowe kolumny

            if new_data.empty:  # jeśli istnieją zmiany to je dodaj do pliku
                new_data.to_csv(cancelations_transformed_data_new.uri, index=False)

                source.append(new_data)
                source.to_csv(cancelations_transformed_data.uri, index=False)

        return df

    @task()
    def add_changes_to_time_table(df: pd.DataFrame):
        # mamy tabele ..new.csv aby do bazy danych wysłać tylko nowe rekody
        if not os.path.exists(time_transformed_data.uri):

            df.to_csv(time_transformed_data.uri, index=False)
            df.to_csv(time_transformed_data_new.uri, index=False)

        else:

            source = pd.read_csv(time_transformed_data.uri)
            new_data = show_new_cols(source, df)  # fukncja znajdująca nowe kolumny

            if new_data.empty:  # jeśli istnieją zmiany to je dodaj do pliku
                new_data.to_csv(time_transformed_data_new.uri, index=False)

                source.append(new_data)
                source.to_csv(time_transformed_data.uri, index=False)

        return df

    @task()
    def add_changes_to_delay_table(df: pd.DataFrame):
        # mamy tabele ..new.csv aby do bazy danych wysłać tylko nowe rekody
        if not os.path.exists(delay_transformed_data.uri):

            df.to_csv(delay_transformed_data.uri, index=False)
            df.to_csv(delay_transformed_data_new.uri, index=False)

        else:

            source = pd.read_csv(delay_transformed_data.uri)
            new_data = show_new_cols(source, df)  # fukncja znajdująca nowe kolumny

            if new_data.empty:  # jeśli istnieją zmiany to je dodaj do pliku
                new_data.to_csv(delay_transformed_data_new.uri, index=False)

                source.append(new_data)
                source.to_csv(delay_transformed_data.uri, index=False)

        return df

    @task()
    def add_changes_to_delay_table(df: pd.DataFrame):
        # mamy tabele ..new.csv aby do bazy danych wysłać tylko nowe rekody
        if not os.path.exists(delay_transformed_data.uri):

            df.to_csv(delay_transformed_data.uri, index=False)
            df.to_csv(delay_transformed_data_new.uri, index=False)

        else:

            source = pd.read_csv(delay_transformed_data.uri)
            new_data = show_new_cols(source, df)  # fukncja znajdująca nowe kolumny

            if new_data.empty:  # jeśli istnieją zmiany to je dodaj do pliku
                new_data.to_csv(delay_transformed_data_new.uri, index=False)

                source.append(new_data)
                source.to_csv(delay_transformed_data.uri, index=False)

        return df

    @task()
    def add_changes_to_flikghts_table(df: pd.DataFrame):
        # mamy tabele ..new.csv aby do bazy danych wysłać tylko nowe rekody
        if not os.path.exists(flights_transformed_data.uri):

            df.to_csv(flights_transformed_data.uri, index=False)
            df.to_csv(flights_transformed_data_new.uri, index=False)

        else:

            source = pd.read_csv(flights_transformed_data.uri)
            new_data = show_new_cols(source, df)  # fukncja znajdująca nowe kolumny

            if new_data.empty:  # jeśli istnieją zmiany to je dodaj do pliku
                new_data.to_csv(flights_transformed_data_new.uri, index=False)

                source.append(new_data)
                source.to_csv(flights_transformed_data.uri, index=False)

        return df

    @task()
    def check_flight_data_validity(df: pd.DataFrame):
        for column in df:
            columnSeriesObj = df[column]
            print('Column Name : ', column)
            print('Column Contents : ', columnSeriesObj.values)
            val = columnSeriesObj.values

            if column == "flight_id_pk":
                # check for duplicates and type
                if len(val) == len(set(val)) and is_integer_dtype(df[column]):
                    print("dane poprawne w ", column)
                else:
                    print("nieprawdłowe dane w ", column)

            elif column in ["arrival_airport_id_fk", "destination_airport_id_fk", "delay_id_fk"]:
                if is_integer_dtype(df[column]):
                    print("dane poprawne w ", column)
                else:
                    print("nieprawdłowe dane w ", column)

            elif column == "date_id_fk":
                if is_datetime64_any_dtype(df[column]):
                    print("dane poprawne w ", column)
                else:
                    print("nieprawdłowe dane w ", column)

            elif column == "cancelation_id_fk":
                if is_string_dtype(df[column]):
                    print("dane poprawne w ", column)
                else:
                    print("nieprawdłowe dane w ", column)

            elif column in ["departure_time_fk", "departure_final_time_fk", "arrival_time_fk", "departure_delay", "arrival_delay"]:
                if is_string_dtype(df[column]) and sum(1 for number in column if number < 0) == 0:
                    print("dane poprawne w ", column)
                else:
                    print("nieprawdłowe dane w ", column)
        return df

    @task()
    def check_date_data_validity(df: pd.DataFrame):
        for column in df:
            columnSeriesObj = df[column]
            print('Column Name : ', column)
            print('Column Contents : ', columnSeriesObj.values)
            val = columnSeriesObj.values

            if column == "date_id_pk":
                # check for duplicates and type
                if len(val) == len(set(val)) and is_integer_dtype(df[column]) and sum(1 for number in val if number < 0) == 0:
                    print("dane poprawne w ", column)
                else:
                    print("nieprawdłowe dane w ", column)

            elif column == "day":
                if is_integer_dtype(df[column]) and sum(1 for d in val if 0 < d < 32) == len(val):
                    print("dane poprawne w ", column)
                else:
                    print("nieprawdłowe dane w ", column)

            elif column == "month":
                if is_integer_dtype(df[column]) and sum(1 for d in val if 0 < d < 13) == len(val):
                    print("dane poprawne w ", column)
                else:
                    print("nieprawdłowe dane w ", column)

            elif column == "year":
                if is_integer_dtype(df[column]) and sum(1 for d in val if d <= datetime.now().year) == len(val):
                    print("dane poprawne w ", column)
                else:
                    print("nieprawdłowe dane w ", column)

            elif column in ["is_work_day", "is_weekday"]:
                if is_bool_dtype(df[column]):
                    print("dane poprawne w ", column)
                else:
                    print("nieprawdłowe dane w ", column)

            elif column == "quarter":
                if is_integer_dtype(df[column]) and sum(1 for d in val if 0 < d < 4) == len(val):
                    print("dane poprawne w ", column)
                else:
                    print("nieprawdłowe dane w ", column)

            elif column == "full_date":
                if is_datetime64_any_dtype(df[column]):
                    print("dane poprawne w ", column)
                else:
                    print("nieprawdłowe dane w ", column)
        return df

    @task()
    def check_airports_data_validity(df: pd.DataFrame):

        for column in df:
            columnSeriesObj = df[column]
            print('Column Name : ', column)
            print('Column Contents : ', columnSeriesObj.values)
            val = columnSeriesObj.values

            if column == "airport_id_pk":
                if len(val) == len(set(val)) and is_integer_dtype(df[column]) and sum(1 for number in val if number < 0) == 0:
                    print("dane poprawne w ", column)
                else:
                    print("nieprawdłowe dane w ", column)

            elif column == "airport_code":
                if is_string_dtype(df[column]) and sum(1 for string in val if len(string) == 3) == sum(1 for string in val if (string.isupper() or string.isnumeric())) == len(val):
                    print("dane poprawne w ", column)
                else:
                    print("nieprawdłowe dane w ", column)

            elif column in ["city", "country_or_state", "name"]:
                if is_string_dtype(df[column]):
                    print("dane poprawne w ", column)
                else:
                    print("nieprawdłowe dane w ", column)
        return df

    @task()
    def check_aircarriers_data_validity(df: pd.DataFrame):
        for column in df:
            columnSeriesObj = df[column]
            print('Column Name : ', column)
            print('Column Contents : ', columnSeriesObj.values)
            val = columnSeriesObj.values

            if column == "air_carrier_id_pk":
                if len(val) == len(set(val)) and is_integer_dtype(df[column]):
                    print("dane poprawne w ", column)
                else:
                    print("nieprawdłowe dane w ", column)

            elif column == "name":
                if is_string_dtype(df[column]):
                    print("dane poprawne w ", column)
                else:
                    print("nieprawdłowe dane w ", column)

            elif column == "shortcut":
                if is_string_dtype(df[column]) and sum(1 for string in val if 1 < len(string) < 4) == sum(1 for string in val if (string.isupper() or string.isnumeric())) == 0:
                    print("dane poprawne w ", column)
                else:
                    print("nieprawdłowe dane w ", column)
        return df

    @task()
    def check_delay_data_validity(df: pd.DataFrame):
        for column in df:
            columnSeriesObj = df[column]
            print('Column Name : ', column)
            print('Column Contents : ', columnSeriesObj.values)
            val = columnSeriesObj.values

            if column == "delay_id_pk":
                if len(val) == len(set(val)) and is_integer_dtype(df[column]):
                    print("dane poprawne w ", column)
                else:
                    print("nieprawdłowe dane w ", column)

            elif column in ["carrier_delay", "weather_delay", "nas_delay", "security_delay", "late_aircraft_delay", "other_type_delay"]:
                if is_integer_dtype(df[column]) and sum(1 for number in val if number < 0) == 0:
                    print("dane poprawne w ", column)
                else:
                    print("nieprawdłowe dane w ", column)
        return df

    @task()
    def check_time_data_validity(df: pd.DataFrame):
        for column in df:
            columnSeriesObj = df[column]
            print('Column Name : ', column)
            print('Column Contents : ', columnSeriesObj.values)
            val = columnSeriesObj.values

            if column == "time_id_pk":
                if len(val) == len(set(val)) and is_integer_dtype(df[column]):
                    print("dane poprawne w ", column)
                else:
                    print("nieprawdłowe dane w ", column)

            elif column == "full_time":
                if is_datetime64_any_dtype(df[column]):
                    print("dane poprawne w ", column)
                else:
                    print("nieprawdłowe dane w ", column)

            elif column == "hour":
                if is_integer_dtype(df[column]) and sum(1 for d in val if 0 <= d < 24) == len(val):
                    print("dane poprawne w ", column)
                else:
                    print("nieprawdłowe dane w ", column)

            elif column == "time_of_the_day":
                if is_string_dtype(df[column]) and sum(1 for string in val if len(string) > 0) == len(val):
                    print("dane poprawne w ", column)
                else:
                    print("nieprawdłowe dane w ", column)
        return df

    data = load_data_nationwide()
    data_carriers = load_air_carriers()
    data_airports = load_data_airports()

    air_carriers = tranform_air_carriers(data_carriers)
    airports = tranform_airports(data_airports)
    times = transform_time_table(data)
    dates = transform_date_table(data)
    delays = transform_delays_table(data)
    cancelations = transform_cancelations_table(data)

    air_carriers1 = check_aircarriers_data_validity(air_carriers)
    airports1 = check_airports_data_validity(airports)
    times1 = check_time_data_validity(times)
    dates1 = check_date_data_validity(dates)
    delays1 = check_delay_data_validity(delays)


    save_date = add_changes_to_date_table(dates1)
    save_air_carriers = add_changes_to_air_carriers_table(air_carriers1)
    save_airports = add_changes_to_airports_table(airports1)
    save_cancelations = add_changes_to_cancelations_table(cancelations)
    save_times = add_changes_to_time_table(times1)
    save_delay = add_changes_to_delay_table(delays1)

    flights = transform_flight_table(data, save_airports, save_date, save_delay)
    flights1 = check_flight_data_validity(flights)
    save_flights = add_changes_to_flikghts_table(flights1)


etl_process()
