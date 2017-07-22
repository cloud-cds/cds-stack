import asyncio
import datetime as dt
import etl.io_config.server_protocol as protocol
import logging

class Predictor:
    def __init__(self, reader, writer, status, index, is_active):
        self.id = "predictor {}-{}".format(index, 'active' if is_active else 'backup')
        self.reader = reader
        self.writer = writer
        self.status = status        # The last status message from the predictor
        self.index = index          # The partition index
        self.is_active = is_active  # active or backup (boolean)
        self.last_updated = dt.datetime.now()

    def send(self):
        pass

    def recv(self):
        pass

    def print(self):
        logging.info('''
            Predictor:
                id: {}
                index: {}
                status: {}
                is_active: {}
                last_updated: {}
            '''.format(self.id, self.index, self.status, self.is_active, self.last_updated)
            )


class PredictorManager:
    def __init__(self, alert_message_queue):
        self.predictors = {}
        self.alert_message_queue = alert_message_queue


    async def register(self, reader, writer, msg):
        ''' Register connection from a predictor '''

        # Create predictor object
        pred = Predictor(reader, writer, msg['status'], msg['index'], msg['is_active'])

        # Save predictor in data structure
        self.predictors[pred.id] = pred

        # Start listener loop
        return await self.listen(pred)


    async def listen(self, pred):
        ''' Listen for messages from predictor '''
        while True:
            try:
                message = await protocol.read_message(pred.reader, pred.writer)
            except Exception as e:
                print(e)
                return

            if message == protocol.CONNECTION_CLOSED:
                logging.error('Connection to {} closed'.format(pred.id))
                # TODO: change status in data structure
                return

            print(message)
            pred.status = message.get('status')
            pred.last_updated = dt.datetime.now()
            pred.print()

            if message.get('status') == 'IDLE':
                pass

            elif message.get('status') == 'BUSY':
                pass

            elif message.get('status') == 'DEAD':
                pass

            elif message.get('status') == 'FIN':
                pass

            else:
                logging.error("Can't process this message")

    async def handle_etl_done(self):
        # TODO: remove running tasks for last etl round
        # TODO: enqueue new tasks for this etl round

    # def send_to_predictors(self):
    #     for predictor_list in self.predictors:
    #         for predictor in predictor_list:
