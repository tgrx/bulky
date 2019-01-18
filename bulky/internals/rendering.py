from multiprocessing import Process, Queue

from jinja2 import Template

from bulky import conf
from bulky.internals import sql
from bulky.internals import utils


class InsertQueryRenderer(Process):
    def __init__(self, bundles_queue: Queue, queries_queue: Queue):
        """
        Initializes worker which renders INSERT queries
        """

        super().__init__()

        self.__bundles_q = bundles_queue
        self.__queries_q = queries_queue

    def run(self):
        while True:
            bundle = self.__bundles_q.get()

            # stop processing if got poison pill or empty values

            if not bundle or not bundle.values_list:
                if bundle and not bundle.values_list:
                    self.__queries_q.put(None)

                break

            # convert values to db-friendly format

            values_list = list(
                {
                    k: utils.to_db_literal(v, cast_to=bundle.column_types[k])
                    for k, v in values.items()
                }
                for values in bundle.values_list
            )

            # render query

            table = conf.Base.metadata.tables.get(bundle.table)

            template = Template(sql.STMT_INSERT)

            query = template.render(
                table=table.name,
                columns=bundle.columns,
                values_list=values_list,
                returning=bundle.returning,
            )

            self.__queries_q.put(query)


class UpdateQueryRenderer(Process):
    def __init__(self, bundles_queue: Queue, queries_queue: Queue):
        """
        Initializes worker which renders UPDATE queries
        """

        super().__init__()

        self.__bundles_q = bundles_queue
        self.__queries_q = queries_queue

    def run(self):
        while True:
            bundle = self.__bundles_q.get()

            # stop processing if got poison pill or empty values

            if not bundle or not bundle.values_list:
                if bundle and not bundle.values_list:
                    self.__queries_q.put(None)

                break

            # convert values to db-friendly format

            values_list = list(
                {
                    k: utils.to_db_literal(v, cast_to=bundle.column_types[k])
                    for k, v in values.items()
                }
                for values in bundle.values_list
            )

            # render query

            table = conf.Base.metadata.tables.get(bundle.table)

            template = Template(sql.STMT_UPDATE)

            query = template.render(
                src="src",
                dst=table.name,
                columns=bundle.columns,
                values_list=values_list,
                returning=bundle.returning,
                column_types=bundle.column_types,
                columns_to_update=bundle.columns_to_update,
                reference_fields=bundle.reference_fields,
                update_changed=False,
            )

            self.__queries_q.put(query)
