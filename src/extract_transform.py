import sqlite3
import luigi
import pandas as pd


class Chinook(luigi.Task):

    completed = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns = ['albums', 'artists', 'customers', 'employees',
                        'genres', 'invoice_items', 'invoices', 'media_types', 'playlist_track', 'playlists',
                        'sqlite_sequence', 'sqlite_stat1', 'tracks']

    def requires(self):

        return []

    def complete(self):

        return self.task_complete

    def run(self):

        self.completed = True