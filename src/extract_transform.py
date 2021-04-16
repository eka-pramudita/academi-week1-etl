import sqlite3
import luigi
import pandas as pd


class Chinook(luigi.Task):

    completed = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tables = ['albums', 'artists', 'customers', 'employees',
                        'genres', 'invoice_items', 'invoices', 'media_types', 'playlist_track', 'playlists',
                        'sqlite_sequence', 'sqlite_stat1', 'tracks']

    def requires(self):

        return []

    def complete(self):

        return self.completed

    def run(self):

        conn = sqlite3.connect('./data/chinook.db')

        dataset = {}
        for table in self.tables:
            dataset["chinook_{}".format(table)] = pd.read_sql_query("SELECT * FROM {}".format(table), conn)

        # Tracks
        albums_artists = dataset['chinook_albums'].merge(dataset['chinook_artists'], on='ArtistId', how='left')
        tracks = dataset['chinook_tracks'].merge(albums_artists, on='AlbumId', how='left').merge(
            dataset['chinook_genres'], on='GenreId', how='left').merge(
            dataset['chinook_media_types'], on='MediaTypeId', how='left')
        tracks.columns = ['TrackId', 'TrackTitle', 'AlbumId', 'MediaTypeId', 'GenreId', 'Composer', 'Milliseconds', 'Bytes',
                          'UnitPrice', 'AlbumTitle', 'ArtistId', 'Artist', 'Genre', 'MediaType']
        tracks = tracks.drop(['AlbumId', 'MediaTypeId', 'GenreId', 'ArtistId'], axis=1)
        tracks.to_csv('./src/extracted/chinook_tracks.csv', encoding='utf-8', index=False, header=True)

        # Playlist track
        playlist_track = dataset['chinook_playlist_track'].merge(tracks[['TrackId', 'TrackTitle']], on='TrackId', how='left').merge(
            dataset['chinook_playlists'], on='PlaylistId', how='left'
        )
        playlist_track.to_csv('./src/extracted/playlist_track.csv', encoding='utf-8', index=False, header=True)

        # Invoice items
        invoice_items = dataset['chinook_invoice_items'].merge(tracks[['TrackId', 'TrackTitle']], on='TrackId',how='left')
        invoice_items.to_csv('./src/extracted/invoice_items.csv', encoding='utf-8', index=False, header=True)

        # The rest
        dataset['chinook_invoices'].to_csv('./src/extracted/invoices.csv', encoding='utf-8', index=False, header=True)
        dataset['chinook_customers'].to_csv('./src/extracted/customers.csv', encoding='utf-8', index=False, header=True)
        dataset['chinook_employees'].to_csv('./src/extracted/employees.csv', encoding='utf-8', index=False, header=True)

        conn.close()

        self.completed = True


class Database(luigi.Task):

    completed = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tables = ['artists', 'content', 'genres',
                        'labels', 'reviews', 'years']

    def requires(self):

        return []

    def complete(self):

        return self.completed

    def run(self):
        conn = sqlite3.connect('./data/database.sqlite')

        dataset = {}
        for table in self.tables:
            dataset["database_{}".format(table)] = pd.read_sql_query("SELECT * FROM {}".format(table), conn)

        reviews = dataset['database_reviews'].merge(
            dataset['database_content'], on='reviewid', how='left').merge(
            dataset['database_genres'], on='reviewid', how='left').merge(
            dataset['database_labels'], on='reviewid', how='left')
        reviews.to_csv('./src/extracted/database_reviews.csv', encoding='utf-8', index=False, header=True)

        conn.close()

        self.completed = True


class Disaster(luigi.Task):

    completed = False

    def requires(self):

        return []

    def complete(self):

        return self.completed

    def run(self):

        self.completed = True


class Reviews(luigi.Task):

    completed = False

    def requires(self):

        return []

    def complete(self):

        return self.completed

    def run(self):

        self.completed = True


class Users(luigi.Task):

    completed = False

    def requires(self):

        return []

    def complete(self):

        return self.completed

    def run(self):

        self.completed = True


class Tweet(luigi.Task):

    completed = False

    def requires(self):

        return []

    def complete(self):

        return self.completed

    def run(self):

        self.completed = True