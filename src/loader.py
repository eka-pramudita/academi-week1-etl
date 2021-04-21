import sqlite3
import luigi
import pandas as pd
from Scripts.extract_transform import ExtractCompleted


class ChinookLoader(luigi.Task):

    completed = False

    def requires(self):
        return [ExtractCompleted()]

    def complete(self):
        return self.completed

    def run(self):

        conn = sqlite3.connect('academi-etl-luigi.sqlite')

        pd.read_csv('./src/extracted/chinook_tracks.csv').to_sql('chinook_tracks', conn, if_exists="replace", index=False)
        pd.read_csv('./src/extracted/chinook_customers.csv').to_sql('chinook_customers', conn, if_exists="replace", index=False)
        pd.read_csv('./src/extracted/chinook_invoice_items.csv').to_sql('chinook_invoice_items', conn, if_exists="replace", index=False)
        pd.read_csv('./src/extracted/chinook_invoices.csv').to_sql('chinook_invoices', conn, if_exists="replace", index=False)
        pd.read_csv('./src/extracted/chinook_playlist_track.csv').to_sql('chinook_playlist_track', conn, if_exists="replace", index=False)

        conn.close()

        self.completed = True


class DatabaseLoader(luigi.Task):

    completed = False

    def requires(self):
        return [ExtractCompleted()]

    def complete(self):
        return self.completed

    def run(self):

        conn = sqlite3.connect('academi-etl-luigi.sqlite')

        pd.read_csv('./src/extracted/database_reviews.csv').to_sql('database_reviews', conn, if_exists="replace", index=False)

        conn.close()

        self.completed = True


class DisasterLoader(luigi.Task):

    completed = False

    def requires(self):
        return [ExtractCompleted()]

    def complete(self):
        return self.completed

    def run(self):

        conn = sqlite3.connect('academi-etl-luigi.sqlite')

        pd.read_csv('./src/extracted/disaster.csv').to_sql('disaster', conn, if_exists="replace", index=False)

        conn.close()

        self.completed = True


class ReviewsLoader(luigi.Task):

    completed = False

    def requires(self):
        return [ExtractCompleted()]

    def complete(self):
        return self.completed

    def run(self):

        conn = sqlite3.connect('academi-etl-luigi.sqlite')

        pd.read_csv('./src/extracted/reviews.csv').to_sql('reviews', conn, if_exists="replace", index=False)

        conn.close()

        self.completed = True


class TweetLoader(luigi.Task):

    completed = False

    def requires(self):
        return [ExtractCompleted()]

    def complete(self):
        return self.completed

    def run(self):

        conn = sqlite3.connect('academi-etl-luigi.sqlite')

        pd.read_csv('./src/extracted/tweet.csv').to_sql('tweet', conn, if_exists="replace", index=False)

        conn.close()

        self.completed = True


class UserLoader(luigi.Task):

    completed = False

    def requires(self):
        return [ExtractCompleted()]

    def complete(self):
        return self.completed

    def run(self):

        conn = sqlite3.connect('academi-etl-luigi.sqlite')

        pd.read_csv('./src/extracted/user.csv').to_sql('user', conn, if_exists="replace", index=False)

        conn.close()

        self.completed = True


class LoaderCompleted(luigi.Task):

    completed = False

    def requires(self):
        return [ChinookLoader(), DatabaseLoader(), DisasterLoader(), ReviewsLoader(), TweetLoader(), UserLoader()]

    def complete(self):
        return self.completed

    def run(self):

        self.completed = True