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

        disaster = pd.read_csv('./data/disaster_data.csv').to_csv('./src/extracted/disaster.csv',
                                                                  encoding='utf-8', index=False, header=True)

        self.completed = True


class Reviews(luigi.Task):

    completed = False

    def requires(self):

        return []

    def complete(self):

        return self.completed

    def run(self):

        # Compare 2 identical files and check for duplicates, reviews q1
        q1_csv = pd.read_csv('./data/reviews_q1.csv')
        q1_xlsx = pd.read_excel('./data/reviews_q1.xlsx')

        # Adjust date data types
        q1_csv['date'] = pd.to_datetime(q1_csv['date']).dt.date
        q1_xlsx['date'] = pd.to_datetime(q1_xlsx['date']).dt.date

        # Use word count instead of comments
        q1_csv['word_count'] = q1_csv['comments'].str.split().str.len()
        q1_xlsx['word_count'] = q1_xlsx['comments'].str.split().str.len()

        # Drop Duplicates excludes comments
        q1_all = pd.concat([q1_csv.drop('comments', 1),q1_xlsx.drop('comments', 1)])
        q1_all = q1_all.drop_duplicates()
        q1 = q1_all.merge(q1_csv[['id', 'comments']]).drop('word_count', 1)

        # Put together with another quarter
        q2 = pd.read_csv('./data/reviews_q2.csv')
        q3 = pd.read_csv('./data/reviews_q3.csv')
        q4 = pd.read_csv('./data/reviews_q4.csv')
        q2['date'] = pd.to_datetime(q2['date']).dt.date
        q3['date'] = pd.to_datetime(q3['date']).dt.date
        q4['date'] = pd.to_datetime(q4['date']).dt.date

        # Drop Duplicates
        reviews = pd.concat([q1,q2,q3,q4])
        reviews = reviews.drop_duplicates()

        reviews.to_csv('./src/extracted/reviews.csv', encoding='utf-8', index=False, header=True)

        self.completed = True


class Users(luigi.Task):

    completed = False

    def requires(self):

        return []

    def complete(self):

        return self.completed

    def run(self):
        xls = pd.ExcelFile('./data/file_1000.xls')
        user1 = pd.read_excel(xls, 'Sheet1')
        user2 = pd.read_excel(xls, 'Sheet2')

        # Adjust Columns
        user1 = user1.drop(['Unnamed: 0', 'First Name.1'], axis=1)
        user2 = user2.drop('Unnamed: 0', axis=1)

        # Drop Duplicates
        user = pd.concat([user1, user2]).drop_duplicates()

        user.to_csv('./src/extracted/user.csv', encoding='utf-8', index=False, header=True)

        self.completed = True


class Tweet(luigi.Task):

    completed = False

    def requires(self):

        return []

    def complete(self):

        return self.completed

    def run(self):

        tweet = pd.read_json('./data/tweet_data.json', lines=True)
        tweet = tweet.dropna(axis=1, how='all')
        tweet.to_csv('./src/extracted/tweet.csv', encoding='utf-8', index=False, header=True)

        self.completed = True


class ExtractCompleted(luigi.Task):

    completed = False

    def requires(self):

        return [Chinook(), Database(), Disaster(), Users(), Tweet(), Reviews()]

    def complete(self):

        return self.completed

    def run(self):

        self.completed = True