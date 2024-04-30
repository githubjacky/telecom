import cudf
import dask.dataframe as dd
from functools import cached_property
from geopy.distance import geodesic
import pandas as pd
from typing import Literal


class CallDistanceUtil:
    def __init__(self,
                 month: str = '08',
                 frequent_threshold: float = 0.5):
        self.cdr = dd.read_parquet(f'data/processed/2013{month}/cdr_loc')

        # threshold to define the sample whose highest ratio of occurance are over this threshold
        self.frequent_threshold = frequent_threshold


    @cached_property
    def client_lonlat_info(self):
        grouped = (
            self.cdr
            .groupby(['client_nbr', 'cell_id'])
            .size()
            .reset_index()
            .rename(columns={0: 'counts'})
        )
        total_counts = (
            grouped
            .groupby('client_nbr')['counts']
            .sum()
            .reset_index()
            .rename(columns={'counts': 'total_counts'})
        )
        grouped_with_total = grouped.merge(
            total_counts,
            on = 'client_nbr'
        )
        grouped_with_total['ratio'] = (
            grouped_with_total['counts'] 
            / 
            grouped_with_total['total_counts']
        )

        most_frequent_with_ratio = (
            grouped_with_total
            .map_partitions(
                lambda partition: partition.sort_values(
                    by=['client_nbr', 'ratio'],
                    ascending=False
                )
            )
            .drop_duplicates(subset=['client_nbr'], keep='first')
        )

        return (
            most_frequent_with_ratio
            [most_frequent_with_ratio.ratio >= self.frequent_threshold]
            .compute()
            .merge(
                cudf.read_csv('data/processed/meta_tower.csv'),
                on='cell_id'
            )
            [['client_nbr', 'lon', 'lat']]
        )


    @staticmethod
    def calculate_geodesic_distance(df):
        # Create a new DataFrame to store the results
        return df.apply(
                lambda row: geodesic(
                    (row['calling_lat'], row['calling_lon']),
                    (row['called_lat'], row['called_lon'])
                ).kilometers, 
                axis=1
            )


    @cached_property
    def cdr_lonlat(self) -> cudf.DataFrame:
        cdr_lonlat =  (
            self.cdr[['calling_nbr', 'called_nbr']]
            .merge(
                self.client_lonlat_info.rename(
                    columns={
                        'client_nbr': 'calling_nbr', 
                        'lon': 'calling_lon',
                        'lat': 'calling_lat'
                    }
                ), 
                on='calling_nbr'
            )
            .merge(
                self.client_lonlat_info.rename(
                    columns={
                        'client_nbr': 'called_nbr', 
                        'lon': 'called_lon',
                        'lat': 'called_lat'
                    }
                ), 
                on='called_nbr'
            )
            .compute().to_pandas()
        )
        cdr_lonlat['distance'] = self.calculate_geodesic_distance(cdr_lonlat)
        return cudf.from_pandas(cdr_lonlat)


    @cached_property
    def mean_calling_distance(self) -> cudf.DataFrame:
        return (
            self.cdr_lonlat
            [['calling_nbr', 'called_nbr', 'distance']]
            .groupby(['calling_nbr'])['distance']
            .mean()
            .reset_index()
            .rename(columns={'calling_nbr': 'client_nbr', 'distance': 'mean_calling_distance'})
        )


    @cached_property
    def std_calling_distance(self) -> cudf.DataFrame:
        return (
            self.cdr_lonlat
            [['calling_nbr', 'called_nbr', 'distance']]
            .groupby(['calling_nbr'])['distance']
            .std()
            .reset_index()
            .rename(columns={'calling_nbr': 'client_nbr', 'distance': 'std_calling_distance'})
        )


    @cached_property
    def mean_called_distance(self) -> cudf.DataFrame:
        return (
            self.cdr_lonlat
            [['calling_nbr', 'called_nbr', 'distance']]
            .groupby(['called_nbr'])['distance']
            .mean()
            .reset_index()
            .rename(columns={'called_nbr': 'client_nbr', 'distance': 'mean_called_distance'})
        )


    @cached_property
    def std_called_distance(self) -> cudf.DataFrame:
        return (
            self.cdr_lonlat
            [['calling_nbr', 'called_nbr', 'distance']]
            .groupby(['called_nbr'])['distance']
            .std()
            .reset_index()
            .rename(columns={'called_nbr': 'client_nbr', 'distance': 'mean_called_distance'})
        )




    @cached_property
    def mean_communication_distance(self):
        return (
            cudf.concat([
                self.mean_calling_distance.rename(columns={'mean_calling_distance': 'distance'}),
                self.mean_called_distance.rename(columns={'mean_called_distance': 'distance'})
            ])
            .groupby(['client_nbr'])['distance']
            .mean()
            .reset_index()
            .rename(columns={'distance': 'mean_communication_distance'})
        )


    @cached_property
    def std_communication_distance(self):
        return (
            cudf.concat([
                self.std_calling_distance.rename(columns={'std_calling_distance': 'distance'}),
                self.std_called_distance.rename(columns={'std_called_distance': 'distance'})
            ])
            .groupby(['client_nbr'])['distance']
            .std()
            .reset_index()
            .rename(columns={'distance': 'std_communication_distance'})
        )


    def get_user_info(self,
                      target: Literal['communication', 'calling', 'called'],
                      method: Literal['mean', 'std']
                     ) -> pd.DataFrame:
        user_info =  cudf.read_csv('data/processed/201308/clean_user_info.csv')
        match method:
            case 'std':
                match target:
                    case 'calling':
                        return (
                            self.std_calling_distance
                            .merge(user_info, on='client_nbr')
                            .to_pandas()
                        )
                    case 'called':
                        return (
                            self.std_called_distance
                            .merge(user_info, on='client_nbr')
                            .to_pandas()
                        )
                    case _:
                        return (
                            self.std_communication_distance
                            .merge(user_info, on='client_nbr')
                            .to_pandas()
                        )
            case _:  # mean_client_distance
                match target:
                    case 'calling':
                        return (
                            self.mean_calling_distance
                            .merge(user_info, on='client_nbr')
                            .to_pandas()
                        )
                    case 'called':
                        return (
                            self.mean_called_distance
                            .merge(user_info, on='client_nbr')
                            .to_pandas()
                        )
                    case _:
                        return (
                            self.mean_communication_distance
                            .merge(user_info, on='client_nbr')
                            .to_pandas()
                        )
