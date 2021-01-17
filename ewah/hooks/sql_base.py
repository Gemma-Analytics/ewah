from ewah.hooks.base import EWAHBaseHook

from typing import Optional, Dict, Any, Union, List


class EWAHSQLBaseHook(EWAHBaseHook):
    """Base hook extension for use as parent of various SQL hooks.

    Children need the following:
    - _get_db_conn method
    - _get_cursor property
    - _get_dictcursor property
    - execute method
    - get_data_from_sql method
    """

    _DEFAULT_PORT = 1234  # overwrite in child

    @property
    def dbconn(self):
        if not hasattr(self, "_dbconn"):
            if hasattr(self.conn, "ssh_conn_id") and self.conn.ssh_conn_id:
                if not hasattr(self, "_ssh_hook"):
                    self._ssh_hook = EWAHBaseHook.get_hook_from_conn_id(
                        conn_id=self.conn.ssh_conn_id
                    )
                    self.local_bind_address = self._ssh_hook.start_tunnel(
                        self.conn.host, self.conn.port or self._DEFAULT_PORT
                    )
            else:
                self.local_bind_address = self.conn.host, self.conn.port
            self._dbconn = self._get_db_conn()
        return self._dbconn

    @property
    def cursor(self):
        """Cursor that returns lists of lists from the data source."""
        if not hasattr(self, "_cur"):
            self._cur = self._get_cursor()
        return self._cur

    @property
    def dictcursor(self):
        """Cursor that returns lists of dictionaries from the data source."""
        if not hasattr(self, "_dictcur"):
            self._dictcur = self._get_dictcursor()
        return self._dictcur

    def get_records(self, sql, parameters=None):
        """
        Variant of execute method. Required to work with the SQL sensor.
        """
        return self.execute_and_return_result(
            sql=sql, params=parameters, return_dict=False
        )

    def execute_and_return_result(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
        return_dict: bool = False,
    ) -> Union[List[list], List[dict]]:
        cursor = self.dictcursor if return_dict else self.cursor
        self.execute(sql=sql, params=params, commit=False, cursor=cursor)
        return cursor.fetchall()

    def get_data_in_batches(
        self,
        sql: str,
        params: Optional[dict] = None,
        return_dict: bool = True,
        batch_size: int = 100000,
    ):
        cur = self.dictcursor if return_dict else self.cursor
        self.execute(sql, params=params, cursor=cur, commit=False)
        while True:
            self.log.info("Fetching next batch...")
            data = cur.fetchmany(batch_size)
            if data:
                yield data
            else:
                break

    def commit(self):
        self.log.info("Committing changes!")
        return self.dbconn.commit()

    def rollback(self):
        self.log.info("Rolling back changes!")
        return self.dbconn.rollback

    def close(self):
        if hasattr(self, "_cur"):
            self._cur.close()
            del self._cur
        if hasattr(self, "_dictcur"):
            self._dictcur.close()
            del self._dictcur
        if hasattr(self, "_dbconn"):
            if hasattr(self, "_ssh_hook"):
                self._ssh_hook.stop_tunnel()
                del self._ssh_hook
            self._dbconn.close()
            del self._dbconn

    def __del__(self):
        self.close()
